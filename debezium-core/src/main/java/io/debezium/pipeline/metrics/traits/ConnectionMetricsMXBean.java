/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics.traits;

/**
 * Exposes connection metrics.
 */
public interface ConnectionMetricsMXBean {

    /**
     * @return 1 if connected, 0 if not. Exposed as an integer rather than a boolean since the
     * JMX-to-telemetry pipeline cannot ship a native boolean attribute as a GAUGE_INT64 metric.
     */
    long getConnected();
}
