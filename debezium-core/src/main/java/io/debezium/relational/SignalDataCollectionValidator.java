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
 * Validates {@code signal.data.collection} during connector {@code validate()}, turning misconfigurations that
 * otherwise fail silently at streaming time (missing table, wrong FQN shape, wrong column count) into an actionable
 * {@link ConfigValue} error or an audit log line.
 * <p>
 * Gated by the internal {@code signal.data.collection.validation.enabled}/{@code .action} configs so it can be
 * rolled out and rolled back via LaunchDarkly without a code change. Never throws: any failure while probing the
 * database is logged and swallowed, so a bug here cannot strand a connector in provisioning.
 *
 * @author Debezium Authors
 */
public class SignalDataCollectionValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalDataCollectionValidator.class);
    private static final String LOG_PREFIX = "[signal.data.collection.validation]";

    private SignalDataCollectionValidator() {
    }

    /**
     * Runs the check and, depending on {@code signal.data.collection.validation.action}, attaches an error to
     * {@code signalDataCollectionValue}. No-op unless validation is enabled, the connector has one configured,
     * and the source channel that reads it is enabled.
     */
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
            return "Signal data collection '" + rawValue + "' was not found in the database.";
        }

        TableId resolved = matches.stream()
                .filter(connectorConfig::isSignalDataCollection)
                .findFirst()
                .orElse(null);
        if (resolved == null) {
            return "signal.data.collection must be '" + matches.iterator().next() + "' (got '" + rawValue + "').";
        }

        // column.include.list/exclude.list changes which of the signal table's columns actually reach Debezium's
        // schema; counting raw physical columns would be wrong once filtering is configured (a column.include.list
        // that only covers other, unrelated tables reduces the signal table to zero effective columns - a real
        // failure, not something to skip). Reuse the connector's own ColumnNameFilter so the count we validate is
        // exactly the one Debezium's schema builder will use, whether or not filtering is configured.
        ColumnNameFilter columnFilter = connectorConfig.getColumnFilter();
        List<String> physicalColumns = connection.getColumnNames(resolved);
        long effectiveColumnCount = physicalColumns.stream()
                .filter(column -> columnFilter.matches(resolved.catalog(), resolved.schema(), resolved.table(), column))
                .count();
        if (effectiveColumnCount == 3) {
            return null;
        }

        return describeColumnCountProblem(connectorConfig, rawValue, physicalColumns.size(), effectiveColumnCount);
    }

    /**
     * Produces a message that pinpoints which of the distinct column-count failure modes occurred, so it can be
     * told apart at a glance (and grepped for in bulk) without re-deriving the diagnosis from the raw numbers:
     * <ul>
     * <li>no filtering configured, so the table's raw physical shape is simply wrong (too few or too many columns);</li>
     * <li>a filter is configured but matches none of the table's columns (the common, easy-to-hit mistake of a
     * {@code column.include.list} scoped to unrelated tables - silently drops every signal, per CC-42217/CC-43408);</li>
     * <li>a filter is configured and matches some but not all of the 3 required columns (partial coverage); or</li>
     * <li>a filter is configured and matches more than 3 columns (too broad, e.g. a wildcard pattern).</li>
     * </ul>
     */
    private static String describeColumnCountProblem(RelationalDatabaseConnectorConfig connectorConfig, String rawValue,
                                                     int physicalColumnCount, long effectiveColumnCount) {
        if (!connectorConfig.isColumnsFiltered()) {
            return "Signal data collection '" + rawValue + "' has " + physicalColumnCount + " columns but requires exactly 3 (id/type/data); "
                    + "no column.include.list/column.exclude.list is configured, so all " + physicalColumnCount
                    + " columns reach Debezium's schema unfiltered.";
        }

        String activeProperty = Strings.isNullOrBlank(connectorConfig.columnIncludeList()) ? "column.exclude.list" : "column.include.list";

        if (effectiveColumnCount == 0) {
            return "Signal data collection '" + rawValue + "' has 0 of its " + physicalColumnCount + " columns surviving " + activeProperty
                    + " - none are included, so every signal will be silently dropped at runtime. Add this table's id/type/data columns "
                    + "explicitly to " + activeProperty + ".";
        }
        if (effectiveColumnCount < 3) {
            return "Signal data collection '" + rawValue + "' has " + effectiveColumnCount + " of " + physicalColumnCount + " columns surviving "
                    + activeProperty + ", but requires exactly 3 - verify " + activeProperty + " covers all of this table's id/type/data columns.";
        }
        return "Signal data collection '" + rawValue + "' has " + effectiveColumnCount + " columns surviving " + activeProperty
                + ", but requires exactly 3 - narrow " + activeProperty + " to just this table's id/type/data columns.";
    }
}
