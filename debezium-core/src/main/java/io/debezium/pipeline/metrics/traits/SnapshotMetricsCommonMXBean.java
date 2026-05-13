/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics.traits;

import java.util.Map;

/**
 * Snapshot metrics shared by {@link SnapshotMetricsMXBean} and
 * {@link SnapshotMetricsNumericMXBean}. Excludes the state attributes that flip type.
 */
public interface SnapshotMetricsCommonMXBean extends SchemaMetricsMXBean {

    int getTotalTableCount();

    int getRemainingTableCount();

    long getSnapshotDurationInSeconds();

    long getSnapshotPausedDurationInSeconds();

    Map<String, Long> getRowsScanned();

    String getChunkId();

    String getChunkFrom();

    String getChunkTo();

    String getTableFrom();

    String getTableTo();
}
