/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics.traits;

/**
 * Numeric variant of {@link ConnectionMetricsMXBean}, with state attributes exposed as
 * {@code long}. Selected when {@code metrics.numeric.encoding.enable=true}.
 */
public interface ConnectionMetricsNumericMXBean extends ConnectionMetricsCommonMXBean {

    long getConnected();
}
