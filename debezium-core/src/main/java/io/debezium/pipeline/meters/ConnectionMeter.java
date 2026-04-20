/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import java.util.concurrent.atomic.AtomicLong;

import io.debezium.annotation.ThreadSafe;
import io.debezium.pipeline.metrics.traits.ConnectionMetricsMXBean;

/**
 * Carries connection metrics.
 */
@ThreadSafe
public class ConnectionMeter implements ConnectionMetricsMXBean {

    private final AtomicLong connected = new AtomicLong();

    @Override
    public long isConnected() {
        return this.connected.get();
    }

    public void connected(boolean connected) {
        this.connected.set(connected ? 1 : 0);
    }

    public void reset() {
        connected.set(0);
    }
}
