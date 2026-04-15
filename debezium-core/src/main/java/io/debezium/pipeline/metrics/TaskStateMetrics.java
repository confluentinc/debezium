/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.pipeline.metrics;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.metrics.Metrics;
import io.debezium.util.Clock;

/**
 * Metrics for task-level state that is shared across different connector phases
 * (snapshot, streaming, schema recovery, etc.).
 */
@ThreadSafe
public class TaskStateMetrics extends Metrics implements TaskStateMetricsMXBean {

    private volatile long dndActivateAt = Long.MAX_VALUE;
    private final Clock clock;

    public TaskStateMetrics(CdcSourceTaskContext taskContext) {
        this(taskContext, taskContext.getClock());
    }

    // Visible for testing — allows injecting a custom clock
    protected TaskStateMetrics(CdcSourceTaskContext taskContext, Clock clock) {
        super(taskContext, "task");
        this.clock = clock;
    }

    @Override
    public long getConnectTaskDnd() {
        return clock.currentTimeInMillis() >= dndActivateAt ? 1 : 0;
    }

    /**
     * Schedules the do-not-disturb status to activate after the given delay.
     *
     * @param delayMs delay in milliseconds; 0 means activate immediately
     */
    public void scheduleDndAfter(long delayMs) {
        this.dndActivateAt = clock.currentTimeInMillis() + delayMs;
    }

    /**
     * Clears the do-not-disturb status so it reads as 0.
     */
    public void clearDnd() {
        this.dndActivateAt = Long.MAX_VALUE;
    }

    public void reset() {
        dndActivateAt = Long.MAX_VALUE;
    }
}
