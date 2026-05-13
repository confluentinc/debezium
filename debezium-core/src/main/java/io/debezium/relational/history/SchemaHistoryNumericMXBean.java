/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

/**
 * Numeric variant of {@link SchemaHistoryMXBean}, with state attributes exposed as
 * {@code long}. Selected when {@code metrics.numeric.encoding.enable=true}.
 *
 * <p>Note: {@code Status} is renamed to {@code StatusCode} here because Java forbids two
 * getters with the same name and different return types on the same class.
 */
public interface SchemaHistoryNumericMXBean extends SchemaHistoryCommonMXBean {

    /**
     * Encodes the {@link SchemaHistoryMetrics.SchemaHistoryStatus} enum as:
     * {@code 0 = STOPPED}, {@code 1 = RECOVERING}, {@code 2 = RUNNING}.
     */
    long getStatusCode();
}
