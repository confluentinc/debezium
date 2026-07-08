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
import io.debezium.util.LoggingContext;

public class SmartSnapshotConnectorCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmartSnapshotConnectorCoordinator.class);

    private final SnapshotCoordinationFacade snapshotCoordination;
    private final ConnectorContext connectorContext;
    private final String serverName;
    private final long monitorPollIntervalMs;
    // Connector type (MDC connectorType) used to establish the Debezium logging context on the monitor
    // thread. The monitor runs on its own thread, which does not inherit the connector thread's MDC, so
    // without setting it the log-pattern fields (connector type/name) would be blank on monitor lines.
    private final String connectorType;

    // Guards the state machine (smartSnapshotState + currentEpoch + lastNumTasks + lastHandledRestartEpoch),
    // which is read/written by both the monitor thread (monitorIteration) and the connector thread (taskConfigs).
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
     *
     * start() (connector thread) writes it, stop() (another thread) reads it.
     * without volatile stop() could see a stale null and never stop the monitor.
     */
    private volatile Thread monitorThread;

    // Set to true when stop() is called. The monitor checks this so it does not ask for a
    // reconfiguration while the connector is shutting down.
    private volatile boolean stopping = false;

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
                                             long monitorPollIntervalMs,
                                             String connectorType) {
        this.snapshotCoordination = snapshotCoordination;
        this.connectorContext = connectorContext;
        this.serverName = serverName;
        this.monitorPollIntervalMs = monitorPollIntervalMs;
        this.connectorType = connectorType;
    }

    public void start() {
        SourceConnectorContext srcContext = (SourceConnectorContext) connectorContext;
        Map<String, Object> existingOffset = srcContext.offsetStorageReader().offset(Collect.hashMapOf("server", serverName));
        boolean offsetExists = (existingOffset != null);
        boolean snapshotInProgress = offsetExists && isSnapshotInProgress(existingOffset);

        if (offsetExists && !snapshotInProgress) {
            LOGGER.info("Smart snapshot: [role=connector] Existing streaming offset present, skipping smart snapshot");
            this.smartSnapshotState = SmartSnapshotState.COMPLETE;
            return;
        }

        snapshotCoordination.start();

        Map<String, Object> completionInfo = snapshotCoordination.readCompletion();
        if (completionInfo != null) {
            LOGGER.info("Smart snapshot: [role=connector epoch={}] Coordination topic shows snapshot completed, skipping",
                    SnapshotCoordinationFacade.epochOf(completionInfo));
            this.smartSnapshotState = SmartSnapshotState.COMPLETE;
            return;
        }

        // Read the epoch the Connector saved earlier. Do not change it on a plain restart.
        // (The snapshot preparation runs on task-0 after taskConfigs, so the snapshot record may not exist
        // yet at this point — that's why the Connector keeps the epoch in its own record.)
        // On the very first start there is no saved epoch yet; keep the initial value (1).
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
                    LOGGER.info("Smart snapshot: [role=connector epoch={}] Epoch restart. old={} new={}", next, past, next);
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
            LOGGER.info("Smart snapshot: [role=connector epoch={} lastHandledRestartEpoch={}] Snapshot complete, writing completion marker and downscaling", epoch,
                    lastHandledRestartEpoch);
            writeCompletion();
            return null;
        }
        if (epochBumped) {
            // save the new epoch before handing out configs
            LOGGER.info("Smart snapshot: [role=connector epoch={} lastHandledRestartEpoch={}] Epoch bumped, persisting the new epoch", epoch, lastHandledRestartEpoch);
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
        Thread thread = new Thread(() -> {
            // The monitor thread does not inherit the connector thread's MDC; establish it here so the
            // connector type/name appear on monitor log lines.
            LoggingContext.forConnector(connectorType, serverName, "smart-snapshot-monitor");
            LOGGER.info("Smart snapshot: [role=monitor epoch={} lastHandledRestartEpoch={}] Monitor thread started", currentEpoch.get(), lastHandledRestartEpoch);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(monitorPollIntervalMs);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.info("Smart snapshot: [role=monitor epoch={} lastHandledRestartEpoch={}] Monitor thread interrupted, exiting gracefully", currentEpoch.get(),
                            lastHandledRestartEpoch);
                    // todo verify the behaviour if we return here
                    return;
                }
                try {
                    if (monitorIteration()) {
                        LOGGER.info("Smart snapshot: [role=monitor epoch={} lastHandledRestartEpoch={}] Monitor iteration completed, monitor thread will stop now",
                                currentEpoch.get(),
                                lastHandledRestartEpoch);
                        // snapshot completed for this epoch; monitor is done
                        // todo what if the upon finishing the thread exits but our task reconfiguration request is lost?
                        // todo should we continue hitting reconfiguration until downscaling?
                        return;
                    }
                }
                catch (Throwable throwable) {
                    // One bad iteration (for example a malformed record or a transient error) must NOT kill the
                    // monitor. If the monitor dies, the snapshot never downscales or restarts and the connector
                    // hangs silently. So log it and keep polling on the next loop.
                    LOGGER.warn("Smart snapshot: [role=monitor epoch={} lastHandledRestartEpoch={}] Monitor iteration failed, will retry on next poll",
                            currentEpoch.get(),
                            lastHandledRestartEpoch, throwable);
                }
            }
        }, "smart-snapshot-monitor");
        thread.setDaemon(true);

        // If the monitor thread ever dies from something the loop above did not
        // catch, log it and fail the connector so that runtime restarts it, instead of hanging forever.
        // todo verify if this works
        thread.setUncaughtExceptionHandler((t, err) -> {
            LOGGER.error("Smart snapshot: [role=monitor epoch={}] Monitor thread died unexpectedly, failing the connector", currentEpoch.get(), err);
            connectorContext.raiseError(new RuntimeException("Smart snapshot: [role=monitor epoch=" + currentEpoch.get() + "] Monitor thread died", err));
        });

        this.monitorThread = thread;
        thread.start();
    }

    /**
     * One monitor iteration. Returns true when the snapshot is complete (the monitor should stop);
     * false to keep polling.
     */
    boolean monitorIteration() {
        boolean requestReconfiguration = false;
        boolean complete = false;

        // Remember the state before it is changed. If reconfiguration fails below,
        // roll back to these values so the next iteration tries again instead of getting stuck.
        SmartSnapshotState previousState;
        int previousHandledRestartEpoch;
        int epoch;

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

            epoch = currentEpoch.get();
            previousState = smartSnapshotState;
            previousHandledRestartEpoch = lastHandledRestartEpoch;

            // 1. restart_needed — always checked, before the completion check; act on each epoch only once
            boolean restart = false;
            for (int i = 0; i < lastNumTasks; i++) {
                if (snapshotCoordination.isRestartNeeded(String.valueOf(i), epoch)) {
                    LOGGER.info("Smart snapshot: [role=monitor epoch={} lastHandledRestartEpoch={}] task-{} signaled `restart_needed`", epoch,
                            lastHandledRestartEpoch, i);
                    restart = true;
                    break;
                }
            }
            if (restart) {
                // handle each epoch only once
                if (epoch > lastHandledRestartEpoch) {
                    LOGGER.info("Smart snapshot: [role=monitor epoch={}] Detected `restart_needed` signal, bumping the epoch and reconfiguring", epoch);
                    lastHandledRestartEpoch = epoch;
                    smartSnapshotState = SmartSnapshotState.RESTART;
                    requestReconfiguration = true;
                }
                else {
                    LOGGER.info(
                            "Smart snapshot: [role=monitor epoch={} lastHandledRestartEpoch={}] Detected `restart_needed` signal, but ignored since it is not newer than the last restarted epoch",
                            epoch,
                            lastHandledRestartEpoch);
                }
            }
            // Only look at completion when no restart is pending. If the state is already RESTART it means we
            // asked for a reconfiguration that taskConfigs() has not consumed yet — do NOT overwrite it with
            // COMPLETE, or the restart would be silently lost.
            else if (smartSnapshotState != SmartSnapshotState.RESTART) {
                // 2. all complete for the epoch, downscale
                boolean allComplete = true;
                for (int i = 0; i < lastNumTasks; i++) {
                    if (!snapshotCoordination.isTaskDone(String.valueOf(i), epoch)) {
                        allComplete = false;
                        break;
                    }
                }
                if (allComplete) {
                    LOGGER.info("Smart snapshot: [role=monitor epoch={} lastHandledRestartEpoch={}] All {} tasks complete, downscaling", epoch,
                            lastHandledRestartEpoch, lastNumTasks);
                    smartSnapshotState = SmartSnapshotState.COMPLETE;
                    requestReconfiguration = true;
                    complete = true;
                }
            }
        }

        if (requestReconfiguration) {
            // Do not trigger a reconfiguration while shutting down.
            if (stopping) {
                LOGGER.info("Smart snapshot: [role=monitor epoch={} lastHandledRestartEpoch={}] Skipping task reconfiguration, as we are stopping", epoch,
                        lastHandledRestartEpoch);
                return false;
            }
            try {
                connectorContext.requestTaskReconfiguration();
            }
            catch (Exception e) {
                // Undo the state change made above so the next
                // iteration detects the same condition and tries again.
                LOGGER.warn(
                        "Smart snapshot: [role=monitor epoch={} lastHandledRestartEpoch={}] Task reconfiguration request failed, rolling back the state and will retry",
                        epoch,
                        lastHandledRestartEpoch, e);
                synchronized (stateLock) {
                    this.smartSnapshotState = previousState;
                    this.lastHandledRestartEpoch = previousHandledRestartEpoch;
                }
                // keep polling; the monitor must not stop just because one reconfigure call failed
                return false;
            }
        }
        return complete;
    }

    public boolean isComplete() {
        return smartSnapshotState == SmartSnapshotState.COMPLETE;
    }

    public void stop() {
        // avoid reconfiguration while stopping
        stopping = true;
        stopMonitorThread();
        snapshotCoordination.stop();
    }

    private void stopMonitorThread() {
        // read the volatile
        Thread monitorThreadCopy = monitorThread;
        if (monitorThreadCopy != null) {
            monitorThreadCopy.interrupt();
            try {
                monitorThreadCopy.join(5000);
                if (monitorThreadCopy.isAlive()) {
                    LOGGER.warn("Smart snapshot: [role=connector epoch={}] Monitor thread did not stop within 5s", currentEpoch.get());
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            monitorThread = null;
        }
    }

    private void writeCompletion() {
        Map<String, Object> snapshotInfo = snapshotCoordination.readSnapshotInfo();
        String consistentPoint = snapshotInfo != null ? (String) snapshotInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT) : null;
        // Let it throw on failure. The caller (taskConfigs) then does NOT downscale, and the connector fails
        // and restarts — which retries the whole thing cleanly. The producer behind this write already retries
        // transient broker errors internally, so a failure reaching here means it is genuinely not going through.
        snapshotCoordination.writeCompletion(consistentPoint, currentEpoch.get());
    }

    private void persistEpoch(int epoch) {
        // Let it throw on failure: then do not hand out configs at an unsaved epoch (same reasoning as
        // writeCompletion — the underlying producer already retries transient errors).
        snapshotCoordination.writeEpoch(epoch);
    }

    private static boolean isSnapshotInProgress(Map<String, Object> offset) {
        // todo can this be reused from task?
        Object snapshot = offset.get(AbstractSourceInfo.SNAPSHOT_KEY);
        boolean completed = Boolean.TRUE.equals(offset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY));
        return snapshot != null && !completed;
    }

    // visible for testing: lets a test reach the monitor thread (and its uncaught-exception handler)
    Thread monitorThread() {
        return monitorThread;
    }
}
