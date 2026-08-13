/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
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
import io.debezium.util.Clock;
import io.debezium.util.Collect;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;

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

    // Guards the shared state (smartSnapshotState + currentEpoch + lastNumTasks). Two threads touch it: the monitor
    // thread started here, which is the only writer, and whichever runtime thread calls taskConfigs() (the herder
    // thread in Connect, the engine thread when embedded), which only reads. The lock keeps taskConfigs() from
    // seeing a half-applied transition, e.g. the new state with the old epoch. Coordination-topic I/O and
    // requestTaskReconfiguration are done outside this lock so a slow Kafka read never blocks taskConfigs().
    private final Object stateLock = new Object();

    // The monitor thread. start() sets it and stop() reads it, so it is volatile.
    private volatile Thread monitorThread;

    // Current coordination round. Read from the topic in start(), bumped by the monitor thread on a restart,
    // and handed to tasks in taskConfigs().
    private final AtomicInteger currentEpoch = new AtomicInteger(1);

    // Number of tasks, set by taskConfigs(). The monitor needs it to know how many per-task keys to check.
    private volatile int lastNumTasks;

    enum SmartSnapshotState {
        // snapshot is ongoing
        ACTIVE,
        // snapshot is complete
        COMPLETE
    }

    /**
     * What the monitor loop does after an iteration.
     */
    enum MonitorAction {
        // nothing to do, or a restart was requested: keep watching the coordination topic
        CONTINUE_POLLING,
        // the monitor has no work left: the snapshot downscaled, or the connector was failed
        STOP
    }

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

        // An offset with no in-progress snapshot marker means this connector already finished its snapshot and is
        // streaming, so there is nothing to parallelize. This is what covers turning the feature on for a connector
        // that snapshotted before smart snapshot existed (or while it was disabled): there is no completion marker
        // on the coordination topic to find, only the streaming offset.
        if (offsetExists && !snapshotInProgress) {
            LOGGER.info("Smart snapshot: [role=connector] Existing streaming offset present, skipping smart snapshot");
            this.smartSnapshotState = SmartSnapshotState.COMPLETE;
            return;
        }

        snapshotCoordination.start(SnapshotCoordination.MissingTopicPolicy.ASSUME_EXISTS);

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
     * Builds the task list for the current epoch. This only reads the state; the monitor thread owns all state
     * changes and coordination-topic writes. When the snapshot is complete it returns a single config so the tasks
     * downscale to the one streaming task; otherwise it returns one config per task.
     */
    public List<Map<String, String>> taskConfigs(int maxTasks, Map<String, String> baseProps) {
        final int epoch;
        final boolean complete;

        synchronized (stateLock) {
            complete = (smartSnapshotState == SmartSnapshotState.COMPLETE);
            if (!complete) {
                this.lastNumTasks = maxTasks;
            }
            epoch = currentEpoch.get();
        }

        if (complete) {
            LOGGER.info("Smart snapshot: [role=connector epoch={}] Snapshot complete, downscaling to a single task", epoch);
            // Stamp task.id like the ordinary single-task path so the streaming task has a non-null task id;
            // metrics that key on getTaskId() (e.g. SchemaHistoryMetrics in multi-partition mode) NPE otherwise.
            // Deliberately NO epoch/num_tasks: this is a normal streaming task, not a smart-snapshot shard.
            Map<String, String> streamingTaskConfig = new HashMap<>(baseProps);
            streamingTaskConfig.put(ConfigurationNames.TASK_ID_PROPERTY_NAME, "0");
            return Collections.singletonList(streamingTaskConfig);
        }

        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskProps = new HashMap<>(baseProps);
            taskProps.put(ConfigurationNames.TASK_ID_PROPERTY_NAME, String.valueOf(i));
            taskProps.put(SnapshotCoordinationFacade.EPOCH, String.valueOf(epoch));
            taskProps.put(SnapshotCoordinationFacade.NUM_TASKS, String.valueOf(maxTasks));
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    private void startMonitorThread() {
        Thread thread = new Thread(() -> {
            // The monitor runs on its own thread and does not inherit the connector thread's MDC; set it here.
            LoggingContext.forConnector(connectorType, serverName, "smart-snapshot-monitor");
            LOGGER.info("Smart snapshot: [role=monitor epoch={}] Monitor thread started", currentEpoch.get());
            Metronome metronome = Metronome.sleeper(Duration.ofMillis(monitorPollIntervalMs), Clock.SYSTEM);
            while (true) {
                try {
                    metronome.pause();
                    if (monitorIteration() == MonitorAction.STOP) {
                        return;
                    }
                }
                catch (InterruptedException e) {
                    // The only interrupt source is stop(); end the monitor thread.
                    Thread.currentThread().interrupt();
                    LOGGER.info("Smart snapshot: [role=monitor epoch={}] Monitor thread interrupted, stopping", currentEpoch.get());
                    return;
                }
                catch (Throwable t) {
                    // A single bad iteration must not kill the monitor; log and retry on the next poll.
                    LOGGER.warn("Smart snapshot: [role=monitor epoch={}] Monitor iteration failed, will retry", currentEpoch.get(), t);
                }
            }
        }, "smart-snapshot-monitor");
        thread.setDaemon(true);

        // If the monitor thread dies from something the loop did not catch, fail the connector so the runtime
        // restarts it instead of hanging forever.
        thread.setUncaughtExceptionHandler((t, err) -> failConnector("monitor thread died unexpectedly", err));

        this.monitorThread = thread;
        thread.start();
    }

    /**
     * One monitor iteration: decide whether a restart or downscale is needed and, if so, drive the transition (make
     * it durable, change the state, and request the reconfiguration).
     */
    MonitorAction monitorIteration() {
        int epoch;
        int numTasks;
        synchronized (stateLock) {
            // taskConfigs() (which sets lastNumTasks) runs only after start() returns, but the monitor is already
            // polling. Skip until the task count is known, otherwise we would downscale before any task started.
            if (lastNumTasks <= 0) {
                return MonitorAction.CONTINUE_POLLING;
            }
            epoch = currentEpoch.get();
            numTasks = lastNumTasks;
        }

        // Coordination reads are done outside the lock so a slow read never blocks the connector thread.
        boolean restartNeeded = anyTaskNeedsRestart(epoch, numTasks);
        boolean allDone = !restartNeeded && allTasksDone(epoch, numTasks);

        if (restartNeeded) {
            return handleRestart(epoch);
        }
        if (allDone) {
            return handleDownscale(epoch);
        }
        return MonitorAction.CONTINUE_POLLING;
    }

    /**
     * A task needs a restart: bump to a new epoch, persist it, and reconfigure the tasks onto it. The epoch is made
     * durable before the request so that if anything fails the connector restarts cleanly on the new epoch.
     * <p>
     * Note the epoch is bumped and persisted eagerly, before the reconfiguration is applied. If the connector fails
     * here (persist succeeds, then the request fails or the connector dies before the tasks are reconfigured), the
     * persisted epoch is already the new one, N+1, while the tasks and their stored configs are still on N. The
     * restart_needed marker was written on N, so on the next start the monitor does NOT re-detect it at N+1 — and it
     * does not need to. Recovery comes from taskConfigs(): the runtime calls it again on restart, and because it now
     * hands out configs stamped N+1 (different from the stored N) the runtime publishes them and actually restarts the
     * tasks onto N+1. In the meantime any task still on N sees the persisted epoch is ahead of its own and idles (see
     * the stale-epoch check on the task side), so it does no work at the wrong epoch.
     *
     */
    private MonitorAction handleRestart(int epoch) {
        int newEpoch = epoch + 1;
        LOGGER.info("Smart snapshot: [role=monitor epoch={}] Restart needed, bumping epoch from {} to {}", epoch, epoch, newEpoch);
        boolean requested = applyTransition("restart", newEpoch,
                // Persist the new epoch before the runtime hands it to the tasks.
                () -> persistEpoch(newEpoch),
                () -> {
                    currentEpoch.set(newEpoch);
                    smartSnapshotState = SmartSnapshotState.ACTIVE;
                });
        // Restart requested: keep polling, now on the new epoch. Other wise the connector has been failed and the
        // runtime will restart it, so this monitor is done.
        return requested ? MonitorAction.CONTINUE_POLLING : MonitorAction.STOP;
    }

    /**
     * All tasks are done: write the completion marker, mark complete, and reconfigure down to the single streaming
     * task. The marker is made durable before the request so that if anything fails the connector restarts cleanly
     * and skips the snapshot.
     *
     */
    private MonitorAction handleDownscale(int epoch) {
        LOGGER.info("Smart snapshot: [role=monitor epoch={}] All tasks done, snapshot complete, downscaling", epoch);
        applyTransition("downscale", epoch,
                // Write the completion marker before marking complete, so a write failure just retries on the next
                // iteration with the state still ACTIVE.
                this::writeCompletion,
                () -> smartSnapshotState = SmartSnapshotState.COMPLETE);
        // Whether the downscale was requested or the connector was failed, the snapshot is over: nothing left to
        // watch for. On a failure the completion marker is already durable, so the restart skips the snapshot.
        return MonitorAction.STOP;
    }

    /**
     * Drive one state transition: make it durable, apply the state change under the state lock, then ask the runtime
     * to reconfigure the tasks. The durable write always comes first, so a failure anywhere after it leaves the
     * connector able to resume from the coordination topic on the next start.
     * <p>
     * {@code requestTaskReconfiguration()} only submits the request; the runtime applies it later, asynchronously, by
     * calling {@link #taskConfigs}. We do not wait for that to happen — a request the runtime drops is recovered on
     * the next connector restart, because taskConfigs() then hands out configs that differ from the stored ones (a
     * new epoch, or the single downscaled config) and the runtime republishes them.
     *
     * @return true if the reconfiguration was requested; false if the connector was failed instead
     */
    private boolean applyTransition(String action, int epoch, Runnable durableWrite, Runnable stateChange) {
        try {
            durableWrite.run();
        }
        catch (RuntimeException e) {
            failConnector("failed to make " + action + " durable at epoch " + epoch, e);
            return false;
        }
        synchronized (stateLock) {
            stateChange.run();
        }
        LOGGER.info("Smart snapshot: [role=monitor epoch={}] Requesting task reconfiguration for {}", epoch, action);
        try {
            connectorContext.requestTaskReconfiguration();
        }
        catch (RuntimeException e) {
            failConnector("could not request " + action + " reconfiguration", e);
            return false;
        }
        return true;
    }

    /**
     * Fail the connector so the runtime restarts it. Recovery is clean because the epoch and the completion marker
     * are made durable before they are acted on.
     */
    private void failConnector(String reason, Throwable cause) {
        LOGGER.error("Smart snapshot: [role=monitor epoch={}] {}, failing the connector", currentEpoch.get(), reason, cause);
        connectorContext.raiseError(new RuntimeException("Smart snapshot: " + reason, cause));
    }

    private boolean anyTaskNeedsRestart(int epoch, int numTasks) {
        for (int i = 0; i < numTasks; i++) {
            if (snapshotCoordination.isRestartNeeded(String.valueOf(i), epoch)) {
                return true;
            }
        }
        return false;
    }

    private boolean allTasksDone(int epoch, int numTasks) {
        for (int i = 0; i < numTasks; i++) {
            if (!snapshotCoordination.isTaskDone(String.valueOf(i), epoch)) {
                return false;
            }
        }
        return true;
    }

    public boolean isComplete() {
        return smartSnapshotState == SmartSnapshotState.COMPLETE;
    }

    public void stop() {
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
        // Let it throw on failure. The caller (handleDownscale) then does NOT mark complete, and the next monitor
        // iteration retries the write. The producer behind this write already retries transient broker errors
        // internally, so a failure reaching here means it is genuinely not going through.
        snapshotCoordination.writeCompletion(consistentPoint, currentEpoch.get());
    }

    private void persistEpoch(int epoch) {
        // Let it throw on failure. On the restart path the caller (handleRestart) then fails the connector instead
        // of bumping to an unsaved epoch. The underlying producer already retries transient broker errors.
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
