/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.pipeline.meters;

import java.util.concurrent.atomic.AtomicLong;

import io.debezium.annotation.ThreadSafe;

/**
 * Carries task-level state metrics that are shared across different connector phases
 * (snapshot, streaming, schema recovery, etc.).
 */
@ThreadSafe
public class TaskStateMeter {

    private final AtomicLong connectTaskRebalanceExempt = new AtomicLong();

    /**
     * Gets the current rebalance exemption status.
     *
     * @return 1 if the task is exempt from rebalancing, 0 otherwise
     */
    public long getConnectTaskRebalanceExempt() {
        return connectTaskRebalanceExempt.get();
    }

    /**
     * Sets the rebalance exemption status.
     *
     * @param exempt 1 if the task should be exempt from rebalancing, 0 otherwise
     */
    public void setConnectTaskRebalanceExempt(long exempt) {
        connectTaskRebalanceExempt.set(exempt);
    }

    /**
     * Resets the task state meter to its initial state.
     */
    public void reset() {
        connectTaskRebalanceExempt.set(0);
    }
}
