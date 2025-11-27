/*
 * Copyright [2024 - 2025] Confluent Inc.
 */

/*
 * Copyright Debezium Authors.
 *
 * This file contains code derived from the Debezium project, which is licensed
 * under the Apache License, Version 2.0.
 * Modifications have been made to the original code as part of this project.
 */

package io.debezium.pipeline.metrics;

/**
 * Exposes task-level state metrics that are shared across different connector phases
 * (snapshot, streaming, schema recovery, etc.).
 */
public interface TaskStateMetricsMXBean {

    /**
     * Gets the current rebalance exemption status.
     *
     * @return 1 if the task is exempt from rebalancing, 0 otherwise
     */
    long getConnectTaskRebalanceExempt();

    /**
     * Resets the task state metrics to their initial state.
     */
    void reset();
}
