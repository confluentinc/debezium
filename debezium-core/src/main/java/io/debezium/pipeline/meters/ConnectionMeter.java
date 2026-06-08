/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import java.util.concurrent.atomic.AtomicBoolean;

import io.debezium.annotation.ThreadSafe;
import io.debezium.pipeline.metrics.traits.ConnectionMetricsMXBean;

/**
 * Carries connection metrics.
 */
@ThreadSafe
public class ConnectionMeter implements ConnectionMetricsMXBean {

    private final AtomicBoolean connected = new AtomicBoolean();

    @Override
    public boolean isConnected() {
        return this.connected.get();
    }

    @Override
    public long getConnectedCode() {
        return this.connected.get() ? 1L : 0L;
    }

    public void connected(boolean connected) {
        this.connected.set(connected);
    }

    public void reset() {
        connected.set(false);
    }
}
