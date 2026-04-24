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

import io.debezium.config.CommonConnectorConfig;

/**
 * DB-agnostic checks for the signal data collection shape. Each connector's {@code *Connection}
 * class handles the DB-specific identifier resolution and column retrieval, then hands the
 * resolved {@link Column} list here.
 * <p>
 * Checks performed:
 * <ol>
 * <li>column count is exactly three;</li>
 * <li>column names at positions 0/1/2 are {@code id} / {@code type} / {@code data}
 *     (compared case-insensitively, per Debezium documentation which mandates these names).</li>
 * </ol>
 */
public final class SignalDataCollectionChecks {

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
     * Attach the given error messages to the {@link CommonConnectorConfig#SIGNAL_DATA_COLLECTION}
     * {@link ConfigValue}. The per-connection validator short-circuits to an empty list when
     * validation is disabled or unconfigured, so no guard is needed here.
     */
    public static void attach(List<String> errors, Map<String, ConfigValue> configValues) {
        final ConfigValue target = configValues.get(CommonConnectorConfig.SIGNAL_DATA_COLLECTION.name());
        errors.forEach(target::addErrorMessage);
    }
}
