/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.metrics.Metrics;
import io.debezium.pipeline.meters.TaskStateMeter;

/**
 * Metrics for task-level state that is shared across different connector phases
 * (snapshot, streaming, schema recovery, etc.).
 */
@ThreadSafe
public class TaskStateMetrics extends Metrics implements TaskStateMetricsMXBean {

    private final TaskStateMeter taskStateMeter;

    public TaskStateMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "task");
        this.taskStateMeter = new TaskStateMeter();
    }

    @Override
    public long getConnectTaskRebalanceExempt() {
        return taskStateMeter.getConnectTaskRebalanceExempt();
    }

    /**
     * Sets the rebalance exemption status.
     *
     * @param exempt 1 if the task should be exempt from rebalancing, 0 otherwise
     */
    public void setConnectTaskRebalanceExempt(long exempt) {
        taskStateMeter.setConnectTaskRebalanceExempt(exempt);
    }

    @Override
    public void reset() {
        taskStateMeter.reset();
    }
}
