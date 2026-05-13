/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import io.debezium.pipeline.metrics.traits.SnapshotMetricsNumericMXBean;

/**
 * Numeric variant of {@link SnapshotChangeEventSourceMetricsMXBean}, with
 * {@link SnapshotMetricsNumericMXBean} replacing the boolean snapshot state attributes.
 */
public interface SnapshotChangeEventSourceMetricsNumericMXBean extends ChangeEventSourceMetricsMXBean,
        SnapshotMetricsNumericMXBean {
}
