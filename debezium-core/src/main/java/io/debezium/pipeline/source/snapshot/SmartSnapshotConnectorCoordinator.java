/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.ConfigurationNames;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.util.Collect;

public class SmartSnapshotConnectorCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmartSnapshotConnectorCoordinator.class);

    private final SnapshotCoordinationFacade snapshotCoordination;
    private final ConnectorContext connectorContext;
    private final String serverName;
    private final long monitorPollIntervalMs;

    // Guards the state machine (smartSnapshotState + currentEpoch + lastNumTasks + lastHandledRestartEpoch),
    // which is read/written by both the monitor thread (monitorTick) and the connector thread (taskConfigs).
    // Coordination-topic I/O and requestTaskReconfiguration are always done OUTSIDE this lock.
    private final Object stateLock = new Object();

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
    private final AtomicInteger currentEpoch = new AtomicInteger(1);

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
                                             String serverName,
                                             long monitorPollIntervalMs) {
        this.snapshotCoordination = snapshotCoordination;
        this.connectorContext = connectorContext;
        this.serverName = serverName;
        this.monitorPollIntervalMs = monitorPollIntervalMs;
    }

    public void start() {
        SourceConnectorContext srcContext = (SourceConnectorContext) connectorContext;
        Map<String, Object> existingOffset = srcContext.offsetStorageReader().offset(Collect.hashMapOf("server", serverName));
        boolean offsetExists = (existingOffset != null);
        boolean snapshotInProgress = offsetExists && isSnapshotInProgress(existingOffset);

        if (offsetExists && !snapshotInProgress) {
            LOGGER.info("Smart snapshot: Existing streaming offset present, skipping smart snapshot");
            this.smartSnapshotState = SmartSnapshotState.COMPLETE;
            return;
        }

        snapshotCoordination.start();

        Map<String, Object> snapshotInfo = snapshotCoordination.readSnapshotInfo();
        if (snapshotInfo != null
                && Boolean.TRUE.equals(snapshotInfo.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY))) {
            LOGGER.info("Smart snapshot: Coordination topic shows snapshot completed, skipping");
            this.smartSnapshotState = SmartSnapshotState.COMPLETE;
            return;
        }

        // Read the epoch the Connector saved earlier. Do not change it on a plain restart.
        // (The snapshot prep runs on task-0 after taskConfigs, so the snapshot record may not exist
        // yet at this point — that's why the Connector keeps the epoch in its own record.)
        // On the very first start there is no saved epoch yet; keep the initial value (0).
        Integer savedEpoch = snapshotCoordination.readEpoch();
        if (savedEpoch != null) {
            this.currentEpoch.set(savedEpoch);
        }
        persistEpoch(currentEpoch.get());

        startMonitorThread();
    }

    /**
     * Builds the task list. The same inputs (topic + offsets + maxTasks) always give the same output.
     */
    public List<Map<String, String>> taskConfigs(int maxTasks, Map<String, String> baseProps) {
        boolean complete = false;
        boolean epochBumped = false;
        final int numTasks = maxTasks;
        final int epoch;

        // Decide the transition atomically; do the coordination-topic writes afterwards, outside the lock,
        // so a slow Kafka write never blocks the monitor thread.
        synchronized (stateLock) {
            switch (smartSnapshotState) {
                case COMPLETE:
                    complete = true;
                    break;
                case RESTART:
                    int past = currentEpoch.get();
                    int next = currentEpoch.incrementAndGet();
                    LOGGER.info("Smart snapshot: Epoch restart {} -> {}", past, next);
                    this.smartSnapshotState = SmartSnapshotState.ACTIVE;
                    epochBumped = true;
                    break;
                case ACTIVE:
                    break;
            }
            if (!complete) {
                this.lastNumTasks = numTasks;
            }
            epoch = currentEpoch.get();
        }

        if (complete) {
            LOGGER.info("Smart snapshot: Complete for the epoch {}, marking completion and downscaling", epoch);
            writeCompletion();
            return null;
        }
        if (epochBumped) {
            // save the new epoch before handing out configs
            persistEpoch(epoch);
        }

        List<Map<String, String>> out = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            Map<String, String> taskProps = new HashMap<>(baseProps);
            taskProps.put(ConfigurationNames.TASK_ID_PROPERTY_NAME, String.valueOf(i));
            taskProps.put(SnapshotCoordinationFacade.EPOCH, String.valueOf(epoch));
            taskProps.put(SnapshotCoordinationFacade.NUM_TASKS, String.valueOf(numTasks));
            out.add(taskProps);
        }
        return out;
    }

    private void startMonitorThread() {
        monitorThread = new Thread(() -> {
            LOGGER.info("Smart snapshot: Monitor thread started");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(monitorPollIntervalMs);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // todo verify the behaviour if we return here
                    return;
                }
                if (monitorIteration()) {
                    // snapshot completed for this epoch; monitor is done
                    return;
                }
            }
        }, "smart-snapshot-monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    /**
     * One monitor iteration. Returns true when the snapshot is complete (the monitor should stop);
     * false to keep polling.
     */
    boolean monitorIteration() {
        boolean requestReconfiguration = false;
        boolean complete = false;

        // The reads below hit the coordination cache (non-blocking), so the whole decision is taken under the
        // lock; only requestTaskReconfiguration is fired afterwards, outside the lock.
        synchronized (stateLock) {
            // Task count not established yet: taskConfigs() (which sets lastNumTasks) is called by runtime
            // only after start() returns, but the monitor thread is already running. Skip the tick until then —
            // otherwise the completion loop below runs zero iterations, leaves allComplete=true,
            // and would falsely downscale before any task has started
            if (lastNumTasks <= 0) {
                return false;
            }

            int epoch = currentEpoch.get();

            // 1. restart_needed — always checked, before the completion check; act on each epoch only once
            boolean restart = false;
            for (int i = 0; i < lastNumTasks; i++) {
                if (snapshotCoordination.isRestartNeeded(String.valueOf(i), epoch)) {
                    LOGGER.info("Smart snapshot: Task {} restart_needed for the epoch {}", i, epoch);
                    restart = true;
                    break;
                }
            }
            if (restart) {
                // handle each epoch only once
                if (epoch > lastHandledRestartEpoch) {
                    LOGGER.info("Smart snapshot: Detected restart required for the epoch {}, bumping the epoch and reconfiguring", epoch);
                    lastHandledRestartEpoch = epoch;
                    smartSnapshotState = SmartSnapshotState.RESTART;
                    requestReconfiguration = true;
                }
            }
            else {
                // 2. all complete for the epoch → downscale
                boolean allComplete = true;
                for (int i = 0; i < lastNumTasks; i++) {
                    if (!snapshotCoordination.isDone(String.valueOf(i), epoch)) {
                        allComplete = false;
                        break;
                    }
                }
                if (allComplete) {
                    LOGGER.info("Smart snapshot: All {} tasks complete for the epoch {}, downscaling", lastNumTasks, epoch);
                    smartSnapshotState = SmartSnapshotState.COMPLETE;
                    requestReconfiguration = true;
                    complete = true;
                }
            }
        }

        if (requestReconfiguration) {
            connectorContext.requestTaskReconfiguration();
        }
        return complete;
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

    private static boolean isSnapshotInProgress(Map<String, Object> offset) {
        Object snapshot = offset.get(AbstractSourceInfo.SNAPSHOT_KEY);
        boolean completed = Boolean.TRUE.equals(offset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY));
        return snapshot != null && !completed;
    }
}
