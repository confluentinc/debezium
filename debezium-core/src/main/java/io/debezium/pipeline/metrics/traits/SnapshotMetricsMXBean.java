/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics.traits;

import java.util.Map;

/**
 * Exposes snapshot metrics.
 */
public interface SnapshotMetricsMXBean extends SchemaMetricsMXBean {

    int getTotalTableCount();

    int getRemainingTableCount();

    boolean getSnapshotRunning();

    boolean getSnapshotPaused();

    boolean getSnapshotCompleted();

    boolean getSnapshotAborted();

    /**
     * Numeric companion to the four boolean snapshot state attributes:
     * 0=NOT_STARTED, 1=RUNNING, 2=PAUSED, 3=COMPLETED, 4=ABORTED.
     */
    long getSnapshotStatusCode();

    long getSnapshotDurationInSeconds();

    long getSnapshotPausedDurationInSeconds();

    Map<String, Long> getRowsScanned();

    String getChunkId();

    String getChunkFrom();

    String getChunkTo();

    String getTableFrom();

    String getTableTo();
}
