/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.relational.RelationalDatabaseConnectorConfig.SignalDataCollectionValidationAction;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.util.Strings;

/**
 * Validates {@code signal.data.collection} at connector {@code validate()} time, turning silent streaming-time
 * failures (missing table, wrong FQN shape, wrong column count) into a WARN log or a {@link ConfigValue} error,
 * gated by the internal {@code .validation.enabled}/{@code .action} configs. Never throws.
 *
 * @author Debezium Authors
 */
public class SignalDataCollectionValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalDataCollectionValidator.class);
    private static final String LOG_PREFIX = "[signal.data.collection.validation]";

    private SignalDataCollectionValidator() {
    }

    /** No-op unless enabled, {@code signal.data.collection} is set, and the source channel is on; attaches an error only when {@code action=FAIL}. */
    public static void validate(JdbcConnection connection, RelationalDatabaseConnectorConfig connectorConfig, ConfigValue signalDataCollectionValue) {
        if (!connectorConfig.isSignalDataCollectionValidationEnabled()) {
            return;
        }

        String rawValue = connectorConfig.getSignalingDataCollectionId();
        if (Strings.isNullOrBlank(rawValue) || !connectorConfig.getEnabledChannels().contains(SourceSignalChannel.CHANNEL_NAME)) {
            return;
        }

        try {
            String problem = checkSignalDataCollection(connection, connectorConfig, rawValue);
            if (problem == null) {
                return;
            }

            LOGGER.warn("{} {}", LOG_PREFIX, problem);
            if (connectorConfig.getSignalDataCollectionValidationAction() == SignalDataCollectionValidationAction.FAIL) {
                signalDataCollectionValue.addErrorMessage(problem);
            }
        }
        catch (SQLException | RuntimeException e) {
            LOGGER.warn("{} Could not validate signal data collection '{}'", LOG_PREFIX, rawValue, e);
        }
    }

    private static String checkSignalDataCollection(JdbcConnection connection, RelationalDatabaseConnectorConfig connectorConfig, String rawValue)
            throws SQLException {
        Set<TableId> matches = connection.resolveSignalDataCollectionTableId(rawValue);
        if (matches.isEmpty()) {
            return "Signal data collection '" + rawValue + "' was not found in the database. Source-channel signaling "
                    + "will not work until this table is created.";
        }

        TableId resolved = matches.stream()
                .filter(connectorConfig::isSignalDataCollection)
                .findFirst()
                .orElse(null);
        if (resolved == null) {
            if (matches.size() == 1) {
                return "signal.data.collection must be '" + matches.iterator().next() + "' (got '" + rawValue + "').";
            }
            List<String> candidates = matches.stream().map(TableId::toString).sorted().toList();
            return "signal.data.collection must be one of: " + candidates + " (got '" + rawValue + "').";
        }

        // Reuse the connector's real ColumnNameFilter so the count matches what Debezium's schema builder sees -
        // e.g. a column.include.list scoped to other tables legitimately reduces this to 0, which must be flagged.
        ColumnNameFilter columnFilter = connectorConfig.getColumnFilter();
        List<String> physicalColumns = connection.getColumnNames(resolved);
        long effectiveColumnCount = physicalColumns.stream()
                .filter(column -> columnFilter.matches(resolved.catalog(), resolved.schema(), resolved.table(), column))
                .count();
        if (effectiveColumnCount == 3) {
            return null;
        }
        if (!connectorConfig.isColumnsFiltered()) {
            return String.format("Signal data collection '%s' has %d columns but requires exactly 3 (id/type/data); no "
                    + "column.include.list/column.exclude.list is configured, so all %2$d columns reach Debezium's schema unfiltered.",
                    rawValue, physicalColumns.size());
        }

        String activeProperty = Strings.isNullOrBlank(connectorConfig.columnIncludeList()) ? "column.exclude.list" : "column.include.list";
        if (effectiveColumnCount == 0) {
            return String.format("Signal data collection '%s' has 0 of its %2$d columns surviving %3$s - none are included, so every "
                    + "signal will be silently dropped at runtime. Add this table's id/type/data columns explicitly to %3$s.",
                    rawValue, physicalColumns.size(), activeProperty);
        }
        if (effectiveColumnCount < 3) {
            return String.format("Signal data collection '%s' has %d of %d columns surviving %s, but requires exactly 3 - verify "
                    + "%4$s covers all of this table's id/type/data columns.",
                    rawValue, effectiveColumnCount, physicalColumns.size(), activeProperty);
        }
        return String.format("Signal data collection '%s' has %d columns surviving %s, but requires exactly 3 - narrow "
                + "%3$s to just this table's id/type/data columns.",
                rawValue, effectiveColumnCount, activeProperty);
    }
}
