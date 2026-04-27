/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.SignalDataCollectionValidationAction;

/**
 * DB-agnostic checks for the signal data collection shape. The JDBC-layer validator resolves the
 * table (via {@code JdbcConnection.resolveSignalDataCollectionTableId}, which connectors may
 * override) and reads its columns, then hands the list here.
 * <p>
 * Checks performed:
 * <ol>
 * <li>column count is exactly three;</li>
 * <li>column names at positions 0/1/2 are {@code id} / {@code type} / {@code data}
 *     (compared case-insensitively, per Debezium documentation which mandates these names).</li>
 * </ol>
 */
public final class SignalDataCollectionChecks {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalDataCollectionChecks.class);

    private static final String[] EXPECTED_COLUMN_NAMES = { "id", "type", "data" };

    private SignalDataCollectionChecks() {
    }

    /**
     * Validate a resolved column list against the required signal-table shape.
     *
     * @param rawId   the user-supplied {@code signal.data.collection} string (for error messages)
     * @param columns the columns of the resolved table, in definition order; never {@code null}
     * @return zero or more user-facing error messages
     */
    public static List<String> validateShape(String rawId, List<Column> columns) {
        if (columns.size() != 3) {
            return Collections.singletonList(
                    "Signal data collection '" + rawId + "' must have exactly 3 columns but has " + columns.size() + ".");
        }

        final List<String> errors = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final Column col = columns.get(i);
            if (!EXPECTED_COLUMN_NAMES[i].equalsIgnoreCase(col.name())) {
                errors.add("Signal data collection '" + rawId + "' column at position " + i
                        + " must be named '" + EXPECTED_COLUMN_NAMES[i] + "' but found '" + col.name() + "'.");
            }
        }
        return errors;
    }

    /**
     * Surface the given error messages either as warnings in the log or as errors on the
     * {@link CommonConnectorConfig#SIGNAL_DATA_COLLECTION} {@link ConfigValue}, depending on the
     * configured action. The per-connection validator short-circuits to an empty list when
     * validation is disabled or unconfigured, so no guard is needed here.
     */
    public static void attach(List<String> errors, Map<String, ConfigValue> configValues, SignalDataCollectionValidationAction action) {
        if (errors.isEmpty()) {
            return;
        }
        LOGGER.warn("[signal.data.collection.validation] The table configured for signaling is not properly set up; {} issue(s) found: {}.",
                errors.size(), String.join(" | ", errors));
        if (action == SignalDataCollectionValidationAction.FAIL) {
            final ConfigValue target = configValues.get(CommonConnectorConfig.SIGNAL_DATA_COLLECTION.name());
            errors.forEach(target::addErrorMessage);
        }
    }
}
