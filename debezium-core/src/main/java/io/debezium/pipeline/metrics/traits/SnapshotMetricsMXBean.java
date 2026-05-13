/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics.traits;

/**
 * Original variant of snapshot metrics, with boolean-typed state attributes.
 */
public interface SnapshotMetricsMXBean extends SnapshotMetricsCommonMXBean {

    boolean isSnapshotRunning();

    boolean isSnapshotPaused();

    boolean isSnapshotCompleted();

    boolean isSnapshotAborted();
}
