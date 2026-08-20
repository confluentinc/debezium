/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.SQLException;
import java.util.Set;

import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.relational.RelationalDatabaseConnectorConfig.SignalDataCollectionValidationAction;
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

        int columnCount = connection.getColumnCount(resolved);
        if (columnCount != 3) {
            return "Signal data collection '" + rawValue + "' must have exactly 3 columns but has " + columnCount + ".";
        }

        return null;
    }
}
