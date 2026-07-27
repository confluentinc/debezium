/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.metrics;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * Extended metrics exposed by the MongoDB connector during streaming.
 *
 * @author Chris Cranford
 */
public interface MongoDbStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    /**
     * Numeric surrogate for {@link #isConnected()} (1 = connected, 0 = disconnected), additive
     * alongside the existing boolean attribute. The JMX-to-telemetry pipeline cannot ship a native
     * boolean attribute as a GAUGE_INT64 metric, so this exposes the same state as an integer for
     * the streaming_is_connected telemetry metric to be sourced from, without changing the type of
     * the existing Connected attribute (which is already relied upon by Confluent Platform users).
     */
    long getConnectedCode();

    long getNumberOfDisconnects();

    long getNumberOfPrimaryElections();

    long getLastSourceEventPollTime();

    long getLastEmptyPollTime();

    long getNumberOfEmptyPolls();
}
