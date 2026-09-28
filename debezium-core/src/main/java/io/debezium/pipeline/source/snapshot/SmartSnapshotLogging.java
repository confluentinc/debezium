/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

/**
 * The prefix every smart snapshot log line and exception message starts with, e.g.
 * {@code Smart snapshot: [role=task taskId=1 epoch=3]}. It says which component wrote the line and, where known, which
 * task and which round (epoch) it refers to, so the lines of one round can be followed across the connector, the
 * monitor, the leader and every task. Built in one place so the format cannot drift between classes and modules.
 *
 * <p>There is one method per role, each taking exactly the fields that role logs, so a role name cannot be mistyped.
 * The constants are for lines written before (or without) a task id / epoch.
 */
public final class SmartSnapshotLogging {

    private static final String PREFIX = "Smart snapshot: ";

    /**
     * The coordination-topic layer; it is shared by every role, so it carries no task or epoch.
     */
    public static final String COORDINATION = PREFIX + "[role=coordination]";

    /**
     * The connector, before it has read the epoch (e.g. while deciding whether smart snapshot applies at all).
     */
    public static final String CONNECTOR = PREFIX + "[role=connector]";

    /**
     * A task that is not a smart snapshot data task, e.g. the post-downscale streaming task.
     */
    public static final String TASK = PREFIX + "[role=task]";

    private SmartSnapshotLogging() {
    }

    /**
     * The connector, once the epoch is known.
     */
    public static String connector(int epoch) {
        return PREFIX + "[role=connector epoch=" + epoch + "]";
    }

    /**
     * The connector's monitor thread.
     */
    public static String monitor(int epoch) {
        return PREFIX + "[role=monitor epoch=" + epoch + "]";
    }

    /**
     * The leader (task-0's background thread that prepares and publishes the shared snapshot).
     */
    public static String leader(int epoch) {
        return PREFIX + "[role=leader epoch=" + epoch + "]";
    }

    /**
     * A smart snapshot data task, before its epoch has been read from the config.
     */
    public static String task(String taskId) {
        return PREFIX + "[role=task taskId=" + taskId + "]";
    }

    /**
     * A smart snapshot data task.
     */
    public static String task(String taskId, int epoch) {
        return PREFIX + "[role=task taskId=" + taskId + " epoch=" + epoch + "]";
    }
}
