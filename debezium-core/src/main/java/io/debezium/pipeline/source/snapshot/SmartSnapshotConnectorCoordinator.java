/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.ConfigurationNames;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

public class SmartSnapshotConnectorCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmartSnapshotConnectorCoordinator.class);
    private static final long MONITOR_POLL_INTERVAL_MS = 30_000;

    private final SnapshotCoordinationFacade snapshotCoordination;
    private final ConnectorContext connectorContext;
    private final String serverName;

    /*
     * Performs 2 monitoring operations:
     * 1. check if any task has signalled for a restart, if yes set snapshot state to RESTART and invoke
     * connectorContext#requestTaskReconfiguration this will cause the snapshot to be restarted with
     * a higher epoch
     * 2. check if all task have completed their snapshot, if yes set snapshotState to COMPLETE
     * and invoke connectorContext#requestTaskReconfiguration this will cause task downscale
     * and streaming to begin
     *
     * check startMonitorThread()
     */
    private Thread monitorThread;

    /*
     * current coordination round
     * for the first time it is set in the start() method by the information read from the coordination topic
     * it is then propagated to the task as in task configs
     * it is updated in taskConfigs() when it detected the snapshotState to be RESTART
     */
    private final AtomicInteger currentEpoch = new AtomicInteger(0);

    /*
     * the number of tasks
     * taskConfigs() sets lastNumTasks to numTasks
     * after that, the monitor knows how many per-task keys to
     * check for completion.
     */
    private volatile int lastNumTasks;

    /*
     * Last epoch which caused a restart
     * Multiple task can signal restart concurrently
     * However, reconfiguration should be triggered only once per epoch
     */
    private volatile int lastHandledRestartEpoch = -1;

    enum SmartSnapshotState {
        // smart snapshot is ongoing
        ACTIVE,
        // smart snapshot is completed
        COMPLETE,
        // smart snapshot needs to be restarted from scratch
        // (could be re-started due to task failure or issue in snapshot holder connection)
        RESTART
    }

    // current snapshot state
    private volatile SmartSnapshotState smartSnapshotState = SmartSnapshotState.ACTIVE;

    public SmartSnapshotConnectorCoordinator(SnapshotCoordinationFacade snapshotCoordination,
                                             ConnectorContext connectorContext,
                                             String serverName) {
        this.snapshotCoordination = snapshotCoordination;
        this.connectorContext = connectorContext;
        this.serverName = serverName;
    }

    public void start() {
        SourceConnectorContext srcContext = (SourceConnectorContext) connectorContext;
        Map<String, Object> existingOffset = srcContext.offsetStorageReader().offset(Collect.hashMapOf("server", serverName));
        boolean offsetExists = (existingOffset != null);
        boolean snapshotInProgress = offsetExists && isSnapshotInProgress(existingOffset);

        if (offsetExists && !snapshotInProgress) {
            LOGGER.info("Smart snapshot: existing streaming offset present, skipping smart snapshot");
            this.smartSnapshotState = SmartSnapshotState.COMPLETE;
            return;
        }

        snapshotCoordination.start();

        Map<String, Object> snapshotInfo = snapshotCoordination.readSnapshotInfo();
        if (snapshotInfo != null
                && Boolean.TRUE.equals(snapshotInfo.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY))) {
            LOGGER.info("Smart snapshot: coordination topic shows snapshot completed, skipping");
            this.smartSnapshotState = SmartSnapshotState.COMPLETE;
            return;
        }

        // Read the epoch the Connector saved earlier. Do not change it on a plain restart.
        // (The snapshot prep runs on task-0 after taskConfigs, so the snapshot record may not exist
        // yet at this point — that's why the Connector keeps the epoch in its own record.)
        this.currentEpoch.set(snapshotCoordination.readEpoch());
        persistEpoch(currentEpoch.get());

        startMonitorThread();
    }

    /**
     * Builds the task list. The same inputs (topic + offsets + maxTasks) always give the same output.
     */
    public List<Map<String, String>> taskConfigs(int maxTasks, Map<String, String> baseProps) {

        switch (smartSnapshotState) {
            case COMPLETE:
                LOGGER.info("Smart snapshot: complete, writing completion + downscaling");
                writeCompletion();
                return null;
            case RESTART:
                int past = currentEpoch.get();
                int next = currentEpoch.incrementAndGet();
                persistEpoch(next); // save the new epoch before handing out configs
                LOGGER.info("Smart snapshot: epoch restart {} -> {}", past, next);
                this.smartSnapshotState = SmartSnapshotState.ACTIVE;
                break;
            case ACTIVE:
                break;
        }

        int numTasks = maxTasks;
        this.lastNumTasks = numTasks;
        List<Map<String, String>> out = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            Map<String, String> taskProps = new HashMap<>(baseProps);
            taskProps.put(ConfigurationNames.TASK_ID_PROPERTY_NAME, String.valueOf(i));
            taskProps.put(SnapshotCoordinationFacade.EPOCH, String.valueOf(currentEpoch.get()));
            taskProps.put(SnapshotCoordinationFacade.NUM_TASKS, String.valueOf(numTasks));
            out.add(taskProps);
        }
        return out;
    }

    private void startMonitorThread() {
        monitorThread = new Thread(() -> {
            LOGGER.info("Smart snapshot: monitor thread started");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(MONITOR_POLL_INTERVAL_MS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (lastNumTasks <= 0) {
                    continue;
                }

                int epoch = currentEpoch.get();

                // 1. restart_needed — always checked, before the completion check; act on each epoch only once
                boolean restart = false;
                for (int i = 0; i < lastNumTasks; i++) {
                    boolean restartNeeded = snapshotCoordination.isRestartNeeded(String.valueOf(i), epoch);
                    if (restartNeeded) {
                        LOGGER.info("Smart snapshot: task {} restart_needed for the epoch {}", i, epoch);
                        restart = true;
                        break;
                    }
                }
                if (restart) {
                    lastHandledRestartEpoch = epoch;
                    smartSnapshotState = SmartSnapshotState.RESTART;
                    connectorContext.requestTaskReconfiguration();
                    continue;
                }

                // 2. all complete for the epoch → downscale
                boolean allComplete = true;
                for (int i = 0; i < lastNumTasks; i++) {
                    boolean done = snapshotCoordination.isDone(String.valueOf(i), epoch);
                    if (!done) {
                        allComplete = false;
                        break;
                    }
                }
                if (allComplete) {
                    LOGGER.info("Smart snapshot: all {} tasks complete for the epoch {}, downscaling", lastNumTasks, epoch);
                    smartSnapshotState = SmartSnapshotState.COMPLETE;
                    connectorContext.requestTaskReconfiguration();
                    return;
                }
            }
        }, "smart-snapshot-monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    public boolean isComplete() {
        return smartSnapshotState == SmartSnapshotState.COMPLETE;
    }

    public void stop() {
        stopMonitorThread();
        snapshotCoordination.stop();
    }

    private void stopMonitorThread() {
        if (monitorThread != null) {
            monitorThread.interrupt();
            try {
                monitorThread.join(5000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            monitorThread = null;
        }
    }

    private void writeCompletion() {
        try {
            Map<String, Object> snapshotInfo = snapshotCoordination.readSnapshotInfo();
            snapshotCoordination.writeCompletion(snapshotInfo != null ? (String) snapshotInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT) : null,
                    currentEpoch.get());
        }
        catch (Exception e) {
            LOGGER.error("Smart snapshot: Failed to write completion", e);
        }
    }

    private void persistEpoch(int epoch) {
        try {
            snapshotCoordination.writeEpoch(epoch);
        }
        catch (Exception e) {
            LOGGER.error("Smart snapshot: Failed to save epoch {}", epoch, e);
        }
    }

    /**
     * Deterministic per-task subset: stable sort by name, round-robin by task id.
     */
    public static List<TableId> tablesForTask(List<TableId> allTables, int taskId, int numTasks) {
        List<TableId> sorted = new ArrayList<>(allTables);
        sorted.sort(Comparator.comparing(TableId::toString));
        List<TableId> mine = new ArrayList<>();
        for (int i = taskId; i < sorted.size(); i += numTasks) { // i = taskId, taskId+numTasks, ...
            mine.add(sorted.get(i));
        }
        return mine;
    }

    /**
     * Parse a comma-joined FQN list back to TableIds.
     */
    public static List<TableId> parseTables(String joined) {
        if (joined == null || joined.isEmpty()) {
            return List.of();
        }
        return Arrays.stream(joined.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .map(TableId::parse).collect(Collectors.toList());
    }

    private static boolean isSnapshotInProgress(Map<String, Object> offset) {
        Object snapshot = offset.get(AbstractSourceInfo.SNAPSHOT_KEY);
        boolean completed = Boolean.TRUE.equals(offset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY));
        return snapshot != null && !completed;
    }
}
