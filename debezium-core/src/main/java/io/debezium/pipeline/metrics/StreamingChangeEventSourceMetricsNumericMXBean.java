/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import io.debezium.metrics.activity.ActivityMonitoringMXBean;
import io.debezium.pipeline.metrics.traits.ConnectionMetricsNumericMXBean;
import io.debezium.pipeline.metrics.traits.StreamingMetricsMXBean;

/**
 * Numeric variant of {@link StreamingChangeEventSourceMetricsMXBean}, with
 * {@link ConnectionMetricsNumericMXBean} replacing the boolean state attributes.
 */
public interface StreamingChangeEventSourceMetricsNumericMXBean extends ChangeEventSourceMetricsMXBean,
        ConnectionMetricsNumericMXBean, StreamingMetricsMXBean, ActivityMonitoringMXBean {
}
