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
import io.debezium.util.Threads;

public class SmartSnapshotConnectorCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmartSnapshotConnectorCoordinator.class);

    private final SnapshotCoordinationFacade snapshotCoordination;
    private final ConnectorContext connectorContext;
    private final String serverName;
    private final long monitorPollIntervalMs;
    // How long a fired reconfiguration request may stay unfulfilled before we give up and fail the connector.
    private final long reconfigurationTimeoutMs;
    // Connector type (MDC connectorType) used to establish the Debezium logging context on the monitor
    // thread. The monitor runs on its own thread, which does not inherit the connector thread's MDC, so
    // without setting it the log-pattern fields (connector type/name) would be blank on monitor lines.
    private final String connectorType;

    // Guards the shared state (smartSnapshotState + currentEpoch + lastNumTasks), read by both the monitor thread
    // and the connector thread. The monitor thread is the only writer of the state and epoch. Coordination-topic
    // I/O and requestTaskReconfiguration are done outside this lock.
    private final Object stateLock = new Object();

    // The monitor thread. start() sets it and stop() reads it, so it is volatile.
    private volatile Thread monitorThread;

    // Set to true by stop() so the monitor does not reconfigure while the connector is shutting down.
    private volatile boolean stopping = false;

    // Current coordination round. Read from the topic in start(), bumped by the monitor thread on a restart,
    // and handed to tasks in taskConfigs().
    private final AtomicInteger currentEpoch = new AtomicInteger(1);

    // Number of tasks, set by taskConfigs(). The monitor needs it to know how many per-task keys to check.
    private volatile int lastNumTasks;

    // Set to true by taskConfigs() every time it hands out configs, and cleared by the monitor just before it
    // requests a reconfiguration. The monitor waits for it to turn true again to confirm the runtime applied the
    // request (i.e. actually called taskConfigs()).
    private volatile boolean reconfigurationApplied = false;

    enum SmartSnapshotState {
        // snapshot is ongoing
        ACTIVE,
        // snapshot is complete
        COMPLETE
    }

    private volatile SmartSnapshotState smartSnapshotState = SmartSnapshotState.ACTIVE;

    public SmartSnapshotConnectorCoordinator(SnapshotCoordinationFacade snapshotCoordination,
                                             ConnectorContext connectorContext,
                                             String serverName,
                                             long monitorPollIntervalMs,
                                             long reconfigurationTimeoutMs,
                                             String connectorType) {
        this.snapshotCoordination = snapshotCoordination;
        this.connectorContext = connectorContext;
        this.serverName = serverName;
        this.monitorPollIntervalMs = monitorPollIntervalMs;
        this.reconfigurationTimeoutMs = reconfigurationTimeoutMs;
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
            // Tell the monitor its reconfiguration request was applied (the runtime called taskConfigs()). Done under
            // the lock, together with reading the state, so it reliably means "taskConfigs observed the state the
            // monitor set before it requested" — see the clear in handleRestart/handleDownscale.
            reconfigurationApplied = true;
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
                    if (monitorIteration()) {
                        // snapshot downscaled, or the connector was failed; either way the monitor is done.
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
        thread.setUncaughtExceptionHandler((t, err) -> {
            LOGGER.error("Smart snapshot: [role=monitor epoch={}] Monitor thread died unexpectedly, failing the connector", currentEpoch.get(), err);
            connectorContext.raiseError(new RuntimeException("Smart snapshot: monitor thread died", err));
        });

        this.monitorThread = thread;
        thread.start();
    }

    /**
     * One monitor iteration: decide whether a restart or downscale is needed and, if so, drive the whole
     * reconfiguration (make it durable, change the state, request it, and wait for the runtime to apply it).
     * Returns true when the monitor should stop (downscale applied, or the connector was failed); false to keep
     * polling.
     */
    boolean monitorIteration() throws InterruptedException {
        int epoch;
        int numTasks;
        synchronized (stateLock) {
            // taskConfigs() (which sets lastNumTasks) runs only after start() returns, but the monitor is already
            // polling. Skip until the task count is known, otherwise we would downscale before any task started.
            if (lastNumTasks <= 0) {
                return false;
            }
            epoch = currentEpoch.get();
            numTasks = lastNumTasks;
        }

        // Coordination reads are done outside the lock so a slow read never blocks the connector thread.
        boolean restartNeeded = anyTaskNeedsRestart(epoch, numTasks);
        boolean allDone = !restartNeeded && allTasksDone(epoch, numTasks);

        if (stopping) {
            return false;
        }
        if (restartNeeded) {
            return handleRestart(epoch);
        }
        if (allDone) {
            return handleDownscale(epoch);
        }
        return false;
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
     * @return false when the restart is applied (keep polling the new round); true when the connector was failed.
     */
    private boolean handleRestart(int epoch) throws InterruptedException {
        int newEpoch = epoch + 1;
        try {
            // Persist the new epoch before the runtime hands it to the tasks.
            persistEpoch(newEpoch);
        }
        catch (RuntimeException e) {
            return failConnector("failed to persist epoch " + newEpoch, e);
        }
        synchronized (stateLock) {
            currentEpoch.set(newEpoch);
            smartSnapshotState = SmartSnapshotState.ACTIVE;
            // Clear the applied flag together with the state change, so a taskConfigs() that ran on the old state
            // cannot leave a stale true; only a taskConfigs() that observes this new epoch can set it again.
            reconfigurationApplied = false;
        }
        LOGGER.info("Smart snapshot: [role=monitor epoch={}] Restart needed, epoch bumped from {} to {}", newEpoch, epoch, newEpoch);
        // Re-check after the writes above: stop() may have set stopping while we were persisting. Do not
        // reconfigure while shutting down; the persisted epoch makes the next start resume cleanly.
        if (stopping) {
            return false;
        }
        if (!requestReconfiguration("restart", newEpoch)) {
            return failConnector("could not request restart reconfiguration", null);
        }
        return awaitReconfiguration(false, newEpoch);
    }

    /**
     * All tasks are done: write the completion marker, mark complete, and reconfigure down to the single streaming
     * task. The marker is made durable before the request so that if anything fails the connector restarts cleanly
     * and skips the snapshot.
     *
     * @return true when the downscale is applied (monitor stops) or the connector was failed.
     */
    private boolean handleDownscale(int epoch) throws InterruptedException {
        try {
            // Write the completion marker before marking complete, so a write failure just retries on the next
            // iteration with the state still ACTIVE.
            writeCompletion();
        }
        catch (RuntimeException e) {
            return failConnector("failed to write completion marker", e);
        }
        synchronized (stateLock) {
            smartSnapshotState = SmartSnapshotState.COMPLETE;
            // Clear the applied flag together with the state change (see handleRestart) so only a taskConfigs() that
            // observes COMPLETE can set it again.
            reconfigurationApplied = false;
        }
        LOGGER.info("Smart snapshot: [role=monitor epoch={}] All tasks done, snapshot complete, downscaling", epoch);
        // Re-check after the write above: stop() may have set stopping while we were writing completion. Do not
        // reconfigure while shutting down; the completion marker makes the next start skip the snapshot.
        if (stopping) {
            return false;
        }
        if (!requestReconfiguration("downscale", epoch)) {
            return failConnector("could not request downscale reconfiguration", null);
        }
        return awaitReconfiguration(true, epoch);
    }

    /**
     * Ask the runtime to reconfigure the tasks. The caller has already cleared {@link #reconfigurationApplied} under
     * the state lock, so the wait can detect when the runtime honors the request by calling taskConfigs().
     *
     * @return true if the request was submitted, false if it failed to submit.
     */
    private boolean requestReconfiguration(String action, int epoch) {
        LOGGER.info("Smart snapshot: [role=monitor epoch={}] {} needed, requesting task reconfiguration", epoch, action);
        try {
            connectorContext.requestTaskReconfiguration();
            return true;
        }
        catch (RuntimeException e) {
            // requestTaskReconfiguration() only submits the request; the runtime applies it later, asynchronously,
            // by calling taskConfigs(). So this catch handles only a synchronous submit failure. A request that is
            // submitted but never applied is handled separately by awaitReconfiguration() timing out.
            LOGGER.warn("Smart snapshot: [role=monitor epoch={}] Task reconfiguration request failed to submit", epoch, e);
            return false;
        }
    }

    /**
     * Wait for the runtime to apply the reconfiguration, i.e. for taskConfigs() to run and set
     * {@link #reconfigurationApplied}. If nothing happens within {@link #reconfigurationTimeoutMs} the request was
     * likely dropped, so fail the connector to restart and retry.
     *
     * @return true if the monitor should stop (downscale applied, or the connector was failed); false to keep
     *         polling (restart applied, or shutting down).
     */
    private boolean awaitReconfiguration(boolean downscale, int epoch) throws InterruptedException {
        Metronome metronome = Metronome.sleeper(Duration.ofMillis(monitorPollIntervalMs), Clock.SYSTEM);
        Threads.Timer timer = Threads.timer(Clock.SYSTEM, Duration.ofMillis(reconfigurationTimeoutMs));
        while (!timer.expired()) {
            if (stopping) {
                return false;
            }
            if (reconfigurationApplied) {
                LOGGER.info("Smart snapshot: [role=monitor epoch={}] {} applied", epoch, downscale ? "downscale" : "restart");
                return downscale;
            }
            metronome.pause();
        }
        return failConnector("task reconfiguration was not applied within " + reconfigurationTimeoutMs + " ms", null);
    }

    /**
     * Fail the connector so the runtime restarts it. Recovery is clean because the epoch and the completion marker
     * are made durable before they are acted on.
     *
     * @return true, so the caller stops the monitor.
     */
    private boolean failConnector(String reason, Throwable cause) {
        LOGGER.error("Smart snapshot: [role=monitor epoch={}] {}, failing the connector", currentEpoch.get(), reason, cause);
        connectorContext.raiseError(new RuntimeException("Smart snapshot: " + reason, cause));
        return true;
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
