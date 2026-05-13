/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

/**
 * Schema-history metrics shared by {@link SchemaHistoryMXBean} and
 * {@link SchemaHistoryNumericMXBean}.
 */
public interface SchemaHistoryCommonMXBean {

    /** @return time in epoch seconds when recovery has started */
    long getRecoveryStartTime();

    /** @return number of changes that were read during recovery phase */
    long getChangesRecovered();

    /**
     * @return number of changes that were applied during recovery phase increased by number of
     * changes applied during runtime
     */
    long getChangesApplied();

    /** @return elapsed time in milliseconds since the last change was applied */
    long getMilliSecondsSinceLastAppliedChange();

    /** @return elapsed time in milliseconds since the last record was recovered from history */
    long getMilliSecondsSinceLastRecoveredChange();

    /** @return String representation of the last applied change */
    String getLastAppliedChange();

    /** @return String representation of the last recovered change */
    String getLastRecoveredChange();
}
