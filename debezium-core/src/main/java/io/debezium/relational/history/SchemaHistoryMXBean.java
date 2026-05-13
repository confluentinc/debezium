/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

/**
 * Original variant of schema-history metrics, with String-typed state attributes.
 *
 * @author Jiri Pechanec
 */
public interface SchemaHistoryMXBean extends SchemaHistoryCommonMXBean {

    /**
     * The schema history starts in {@code STOPPED} state.
     * Upon start it transitions to {@code RECOVERING} state.
     * When all changes from stored history were applied then it switches to {@code RUNNING} state.
     * <p>Maps to {@link SchemaHistoryMetrics.SchemaHistoryStatus} enum.
     *
     * @return schema history component state
     */
    String getStatus();
}
