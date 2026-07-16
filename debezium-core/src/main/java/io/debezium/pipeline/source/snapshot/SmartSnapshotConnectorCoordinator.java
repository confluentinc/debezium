/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.time.Duration;
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

    // Guards the state machine (smartSnapshotState + currentEpoch + lastNumTasks), shared by the monitor thread
    // and the connector thread. Coordination-topic I/O and requestTaskReconfiguration are done outside this lock.
    private final Object stateLock = new Object();

    // The monitor thread. start() sets it and stop() reads it, so it is volatile.
    private volatile Thread monitorThread;

    // Set to true by stop() so the monitor does not reconfigure while the connector is shutting down.
    private volatile boolean stopping = false;

    // Current coordination round. Read from the topic in start(), handed to tasks in taskConfigs(),
    // and bumped in taskConfigs() when a restart is consumed.
    private final AtomicInteger currentEpoch = new AtomicInteger(1);

    // Number of tasks, set by taskConfigs(). The monitor needs it to know how many per-task keys to check.
    private volatile int lastNumTasks;

    enum SmartSnapshotState {
        // snapshot is ongoing
        ACTIVE,
        // snapshot is complete
        COMPLETE,
        // snapshot must be restarted from scratch (task failure or snapshot-holder connection issue)
        RESTART
    }

    private volatile SmartSnapshotState smartSnapshotState = SmartSnapshotState.ACTIVE;

    // Are the tasks downscaled for streaming, true once taskConfigs() has downscaled to the single streaming task.
    private volatile boolean downscaled = false;

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
     * Returns either the per-task configs or an explicit request to downscale for the single streaming task.
     */
    public TaskConfigsResult taskConfigs(int maxTasks, Map<String, String> baseProps) {
        boolean complete = false;
        boolean epochBumped = false;
        final int numTasks = maxTasks;
        final int epoch;

        // Decide the transition atomically; do the coordination-topic writes after wards, outside the lock,
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
            // Write the completion marker only once, even if reconfiguration drives us here again.
            if (!downscaled) {
                LOGGER.info("Smart snapshot: [role=connector epoch={}] Snapshot complete, writing completion marker and downscaling", epoch);
                writeCompletion();
                downscaled = true;
            }
            return TaskConfigsResult.downscale();
        }
        if (epochBumped) {
            // save the new epoch before handing out configs
            LOGGER.info("Smart snapshot: [role=connector epoch={}] Epoch bumped, persisting", epoch);
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
        return TaskConfigsResult.configs(out);
    }

    /**
     * Outcome of {@link #taskConfigs}: either the per-task configs, or a request to downscale for streaming.
     */
    public static final class TaskConfigsResult {
        private final boolean downscale;
        private final List<Map<String, String>> configs;

        private TaskConfigsResult(boolean downscale, List<Map<String, String>> configs) {
            this.downscale = downscale;
            this.configs = configs;
        }

        static TaskConfigsResult configs(List<Map<String, String>> configs) {
            return new TaskConfigsResult(false, configs);
        }

        static TaskConfigsResult downscale() {
            return new TaskConfigsResult(true, null);
        }

        public boolean isDownscale() {
            return downscale;
        }

        public List<Map<String, String>> configs() {
            return configs;
        }
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
     * One monitor iteration: decide whether a restart or downscale reconfiguration is needed, request it, and wait
     * for the runtime to apply it. Returns true when the monitor should stop (downscale applied, or the connector
     * was failed because the request was never applied); false to keep polling.
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
        if (!restartNeeded && !allDone) {
            return false;
        }

        boolean requestReconfiguration = false;
        boolean awaitingDownscale = false;
        synchronized (stateLock) {
            // A concurrent taskConfigs() may have bumped the epoch while we were reading; if so, re-evaluate next iteration.
            if (currentEpoch.get() != epoch) {
                return false;
            }
            if (restartNeeded) {
                smartSnapshotState = SmartSnapshotState.RESTART;
                requestReconfiguration = true;
            }
            else if (allDone && smartSnapshotState == SmartSnapshotState.ACTIVE) {
                smartSnapshotState = SmartSnapshotState.COMPLETE;
                requestReconfiguration = true;
                awaitingDownscale = true;
            }
        }

        if (!requestReconfiguration || stopping) {
            return false;
        }

        LOGGER.info("Smart snapshot: [role=monitor epoch={}] {} needed, requesting task reconfiguration",
                epoch, awaitingDownscale ? "downscale" : "restart");
        try {
            connectorContext.requestTaskReconfiguration();
        }
        catch (RuntimeException e) {
            // Request failed; go back to ACTIVE so the next iteration detects the same condition and retries.
            LOGGER.warn("Smart snapshot: [role=monitor epoch={}] Task reconfiguration request failed, will retry", epoch, e);
            synchronized (stateLock) {
                smartSnapshotState = SmartSnapshotState.ACTIVE;
            }
            return false;
        }

        return awaitReconfiguration(awaitingDownscale, epoch);
    }

    /**
     * Wait for the runtime to apply the reconfiguration. A restart is applied when taskConfigs() bumps the epoch;
     * a downscale is applied when taskConfigs() sets {@link #downscaled}. If nothing happens within
     * {@link #reconfigurationTimeoutMs} the request was likely dropped, so fail the connector to restart and retry.
     *
     * @return true if the monitor should stop (downscale applied, or the connector was failed); false to keep
     *         polling (restart applied, or shutting down).
     */
    private boolean awaitReconfiguration(boolean awaitingDownscale, int requestedEpoch) throws InterruptedException {
        Metronome metronome = Metronome.sleeper(Duration.ofMillis(monitorPollIntervalMs), Clock.SYSTEM);
        Threads.Timer timer = Threads.timer(Clock.SYSTEM, Duration.ofMillis(reconfigurationTimeoutMs));
        while (!timer.expired()) {
            if (awaitingDownscale) {
                if (downscaled) {
                    LOGGER.info("Smart snapshot: [role=monitor epoch={}] downscale applied", currentEpoch.get());
                    return true;
                }
            }
            else if (currentEpoch.get() > requestedEpoch) {
                // restart is applied, return false to continue polling for the new round
                LOGGER.info("Smart snapshot: [role=monitor epoch={}] restart applied", currentEpoch.get());
                return false;
            }
            if (stopping) {
                return false;
            }
            metronome.pause();
        }

        LOGGER.error("Smart snapshot: [role=monitor epoch={}] Task reconfiguration not applied within {} ms, failing the connector",
                requestedEpoch, reconfigurationTimeoutMs);
        connectorContext.raiseError(new RuntimeException(
                "Smart snapshot: task reconfiguration was not applied within " + reconfigurationTimeoutMs + " ms"));
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
