/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;
import io.debezium.util.Threads;

/**
 * The task-side coordinator for a smart snapshot data task. This is the DB-agnostic orchestration:
 * write the join marker, wait for the leader to publish the snapshot info on the coordination topic,
 * run the snapshot of this task's slice, then record completion and idle until the connector downscales.
 *
 * <p>Everything that differs per connector is confined to two hooks: {@link #epochOf(OffsetContext)}
 * reads the epoch stamped on a saved offset, and
 * {@link #configureSmartSource(SnapshotChangeEventSource, int, String, String, Object, SnapshotCoordinationFacade)}
 * hands the published snapshot info to the connector's smart snapshot source (which knows how to decode the
 * consistent position and parse the table assignment for its own identifier style).
 *
 * <p>Catch-up streaming is always off here: a smart snapshot task only takes a data snapshot of its slice
 * and never streams. The base {@link ChangeEventSourceCoordinator#executeCatchUpStreaming} already returns
 * {@code false}, so this class does not override it.
 */
public abstract class AbstractSmartSnapshotChangeEventSourceCoordinator<P extends Partition, O extends OffsetContext>
        extends ChangeEventSourceCoordinator<P, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSmartSnapshotChangeEventSourceCoordinator.class);
    private static final int DEFAULT_SNAPSHOT_INFO_POLL_INTERVAL_MS = 10_000;

    protected final int epoch;
    protected final SnapshotCoordinationFacade snapshotCoordination;
    protected final String taskId;
    // How long to wait for the leader to publish the snapshot info before failing this task.
    private final long snapshotInfoWaitTimeoutMs;
    // Interval between snapshot-info poll attempts. Visible for testing so a unit test does not sleep the default.
    private long snapshotInfoPollIntervalMs = DEFAULT_SNAPSHOT_INFO_POLL_INTERVAL_MS;

    protected AbstractSmartSnapshotChangeEventSourceCoordinator(
                                                                Offsets<P, O> previousOffsets,
                                                                ErrorHandler errorHandler,
                                                                Class<? extends SourceConnector> connectorType,
                                                                CommonConnectorConfig connectorConfig,
                                                                ChangeEventSourceFactory<P, O> changeEventSourceFactory,
                                                                ChangeEventSourceMetricsFactory<P> changeEventSourceMetricsFactory,
                                                                EventDispatcher<P, ?> eventDispatcher,
                                                                DatabaseSchema<?> schema,
                                                                SignalProcessor<P, O> signalProcessor,
                                                                NotificationService<P, O> notificationService,
                                                                SnapshotterService snapshotterService,
                                                                int epoch,
                                                                SnapshotCoordinationFacade snapshotCoordination,
                                                                String taskId) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
                changeEventSourceMetricsFactory, eventDispatcher, schema, signalProcessor, notificationService,
                snapshotterService);
        this.epoch = epoch;
        this.snapshotCoordination = snapshotCoordination;
        this.taskId = taskId;
        this.snapshotInfoWaitTimeoutMs = connectorConfig.getSmartSnapshotTaskSnapshotInfoWaitTimeoutMs();
    }

    // Visible for testing: shorten the snapshot-info poll interval so tests do not sleep the default.
    protected void setSnapshotInfoPollIntervalMs(long snapshotInfoPollIntervalMs) {
        this.snapshotInfoPollIntervalMs = snapshotInfoPollIntervalMs;
    }

    /**
     * Read the epoch stamped on a previously saved offset, or {@code null} if none is present. Used to detect
     * a stale offset left over from an earlier round so it can be discarded before this round's snapshot.
     */
    protected abstract Integer epochOf(O offset);

    /**
     * Hand the leader's published snapshot info to the connector's smart snapshot source. The connector decodes
     * the consistent position (Postgres LSN, MySQL binlog file/pos/gtids) and parses {@code assignmentForTask}
     * (the raw per-task table slice) using its own identifier interpretation, then stores everything on the
     * source so the subsequent {@link #doSnapshot} reads at the shared point.
     *
     * @param snapshotName       the leader's snapshot name (Postgres exported snapshot); {@code null} for MySQL
     * @param consistentPoint    the shared consistent position, connector-encoded
     * @param assignmentForTask  this task's raw table slice from the published assignments
     */
    protected abstract void configureSmartSource(SnapshotChangeEventSource<P, O> snapshotSource,
                                                 int epoch,
                                                 String snapshotName,
                                                 String consistentPoint,
                                                 Object assignmentForTask,
                                                 SnapshotCoordinationFacade snapshotCoordination);

    @Override
    protected void executeChangeEventSources(
                                             CdcSourceTaskContext taskContext,
                                             SnapshotChangeEventSource<P, O> snapshotSource,
                                             Offsets<P, O> previousOffsets,
                                             AtomicReference<LoggingContext.PreviousContext> previousLogContext,
                                             ChangeEventSourceContext context)
            throws InterruptedException {

        snapshotCoordination.start();

        P partition = previousOffsets.getTheOnlyPartition();
        previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));

        O previousOffset = previousOffsets.getTheOnlyOffset();

        // Epoch mismatch with previous offset → stale from previous round, clear it
        if (previousOffset != null) {
            Integer offsetEpoch = epochOf(previousOffset);
            if (offsetEpoch != null && !offsetEpoch.equals(epoch)) {
                LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Epoch mismatch, clearing offset. offsetEpoch={} configEpoch={}",
                        taskId, epoch, offsetEpoch, epoch);
                previousOffsets.resetOffset(partition);
                previousOffset = null;
            }
        }

        // previousOffset here is non-null only if its epoch == this epoch (the epoch-mismatch block above
        // reset it otherwise). If it shows the snapshot already completed, this restart is just the task
        // being bounced AFTER finishing (a reconfiguration or the managed runtime stopping a "done" task)
        // NOT a crash. Do not treat it as a rejoin; return and let the connector downscale.
        boolean done = snapshotCoordination.isTaskDone(taskId, epoch);
        if (done) {
            LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Already completed, waiting for downscale", taskId, epoch);
            return;
        }

        // Rejoin detection. Only a task that already STARTED ITS TRANSACTION cannot be resumed after a restart:
        // it attached to the leader's exported snapshot (which may already be released) and, since
        // task_started_transaction is written after the schema read but before any data rows are emitted, it may
        // be mid-slice. That needs a clean round, so signal a full restart.
        //
        // A task that only wrote its join marker but never started its transaction did NOT attach and emitted no
        // data. It can safely re-run at the SAME epoch, so we must NOT force a restart for it. Keying this on the
        // join marker (as before) wrongly bumped the epoch when a task simply died while waiting for the snapshot
        // to be prepared.
        if (snapshotCoordination.isTaskStartedTransaction(taskId, epoch)) {
            LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Rejoin after transaction start detected, signaling `restart_needed`", taskId, epoch);
            writeRestartNeeded();
            return;
        }

        // Stale-epoch check: this task's config epoch is already behind the epoch the connector has persisted.
        // That means the config is stale — a leftover task from an old round, or one assigned during a rebalance
        // before it was reconfigured. Return and wait for the reconfiguration to hand it the current epoch.
        //
        // This must run BEFORE reading the snapshot info. snapshot_info is keyed by server, not by epoch, so a
        // stale task could otherwise find a snapshot whose epoch matches its own stale epoch, attach to it (that
        // snapshot has already been released) and fail. The in-loop epoch check below does NOT cover this: it runs
        // after the snapshot-info match, so a first-iteration match would attach before it ever fires.
        Integer readEpoch = snapshotCoordination.readEpoch();
        if (readEpoch != null && readEpoch > epoch) {
            LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Saved epoch is greater than current epoch, waiting for restart. savedEpoch={} currentEpoch={}",
                    taskId, epoch, readEpoch, epoch);
            return;
        }

        // Write the join marker. It tells the leader this task is up so it can wait for all tasks to join before
        // taking locks. It does NOT by itself trigger a restart on a later bounce — that is keyed on
        // task_started_transaction above — so a task that dies here (joined but not yet attached) re-runs cleanly
        // at the same epoch. If this write fails we just fail the task; the task has done nothing to clean up.
        snapshotCoordination.writeTaskJoin(taskId, epoch);

        // Read snapshot info from the coordination topic. Wait up to snapshotInfoWaitTimeoutMs, which must be
        // larger than the leader's join-wait + prepare time so we do not give up before the snapshot is published.
        String snapshotName = null;
        String consistentPoint = null;
        Object assignmentForTask = null;
        // The loop is interrupt-aware via metronome.pause() below, which throws InterruptedException (propagated by
        // this method) if the task is being stopped.
        Threads.Timer timer = Threads.timer(Clock.SYSTEM, Duration.ofMillis(snapshotInfoWaitTimeoutMs));
        Metronome metronome = Metronome.parker(Duration.ofMillis(snapshotInfoPollIntervalMs), Clock.SYSTEM);
        int attempt = 0;
        boolean ready = false;
        while (!timer.expired()) {
            // A transient coordination read failure here is treated like "not ready yet": log and keep polling
            // until the timeout, instead of failing the task on a single broker blip.
            try {
                Map<String, Object> snapshotInfo = snapshotCoordination.readSnapshotInfo();
                // Readiness keys on the consistent point, which every connector publishes (Postgres also publishes
                // a snapshot name, MySQL does not). Do not key on the snapshot name.
                if (snapshotInfo != null
                        && snapshotInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT) != null) {
                    Integer snapshotInfoEpoch = SnapshotCoordinationFacade.epochOf(snapshotInfo);
                    if (snapshotInfoEpoch != null) {
                        if (snapshotInfoEpoch != epoch) {
                            LOGGER.info(
                                    "Smart snapshot: [role=task taskId={} epoch={}] Received snapshot info is for a different epoch. receivedEpoch={} currentEpoch={}",
                                    taskId, epoch, snapshotInfoEpoch, epoch);
                        }
                        else {
                            snapshotName = (String) snapshotInfo.get(SnapshotCoordinationFacade.SNAPSHOT_NAME);
                            consistentPoint = String.valueOf(snapshotInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT));
                            assignmentForTask = SnapshotCoordinationFacade.assignmentForTask(
                                    snapshotInfo.get(SnapshotCoordinationFacade.ASSIGNMENTS), Integer.parseInt(taskId));
                            ready = true;
                            break;
                        }
                    }
                }

                // The connector may have moved on to a newer epoch (the leader gave up and restarted the round). If so,
                // there is no point waiting out the full timeout for a snapshot at this epoch — return and let this task
                // be reconfigured for the new epoch.
                readEpoch = snapshotCoordination.readEpoch();
                if (readEpoch != null && readEpoch > epoch) {
                    LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Connector advanced to a newer epoch while waiting for snapshot info, "
                            + "waiting for restart. savedEpoch={}", taskId, epoch, readEpoch);
                    return;
                }
            }
            catch (DebeziumException e) {
                LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Transient coordination read failure while waiting for snapshot info, retrying",
                        taskId, epoch, e);
            }

            LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Waiting for snapshot preparation (attempt {}, timeout {}ms)",
                    taskId, epoch, ++attempt, snapshotInfoWaitTimeoutMs);
            metronome.pause();
        }
        if (!ready) {
            throw new DebeziumException(String.format("Smart snapshot: [role=task taskId=%s epoch=%d] Timed out waiting for snapshot preparation", taskId, epoch));
        }

        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Read snapshot info, executing snapshot-only. snapshot={}, position={}",
                taskId, epoch, snapshotName, consistentPoint);

        // Hand the published info to the connector's smart snapshot source (decode position + parse assignment).
        configureSmartSource(snapshotSource, epoch, snapshotName, consistentPoint, assignmentForTask, snapshotCoordination);

        try {
            SnapshotResult<O> snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);
            LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Snapshot completed. status={}", taskId, epoch, snapshotResult.getStatus());
        }
        catch (InterruptedException e) {
            // Interrupt means the task is being stopped/restarted; the snapshot did NOT complete.
            // Do NOT fall through to writeCompleted() — marking an unfinished subset "done" would let the
            // monitor downscale it and cause isTaskDone() to skip the snapshot on the next restart.
            LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Interrupted during snapshot, exiting gracefully", taskId, epoch, e);
            Thread.currentThread().interrupt();
            return;
        }
        catch (Exception e) {
            // An interrupt does not always surface as InterruptedException: a JDBC/socket read or a Kafka
            // producer call can throw a wrapped exception after the interrupt flag was set. Those land here,
            // not in the block above. Treat them like the interrupt path: the task is stopping. On the next
            // start the rejoin path handles cleanup (if it had started its transaction it signals restart;
            // otherwise it re-runs cleanly), and writeRestartNeeded() is a blocking Kafka write that would
            // likely fail under interrupt anyway.
            if (Thread.currentThread().isInterrupted()) {
                LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Interrupted during snapshot (surfaced as {}), exiting gracefully",
                        taskId, epoch, e.getClass().getSimpleName(), e);
                return;
            }

            // A real snapshot failure. Here we DO write restart_needed: the task already attached and may have
            // emitted partial data, so the epoch must bump to throw that work away. The topic is likely still up
            // (the failure was in the snapshot, not the write), so the signal should go through and the monitor
            // acts on its next poll. If the write also fails, writeRestartNeeded throws and the marker handles it
            // on restart.
            LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Snapshot failed, signaling `restart_needed`", taskId, epoch, e);
            writeRestartNeeded();
            throw new DebeziumException(String.format("Smart snapshot: [role=task taskId=%s epoch=%d] Snapshot failed, signaling restart_needed", taskId, epoch), e);
        }

        writeCompleted();

        // Snapshot-only task: nothing more to do here. Return and let the connector monitor detect all tasks done
        // and downscale/reconfigure this task. The task stays alive (RUNNING, empty polls) until then.
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Slice complete, waiting for downscale", taskId, epoch);
    }

    private void writeRestartNeeded() {
        try {
            snapshotCoordination.writeRestartNeeded(taskId, epoch);
        }
        catch (Exception e) {
            // The topic write failed, so we cannot signal a restart. Just fail the task.
            // On restart the marker is still there, so it tries to signal again. If the topic is still
            // down it keeps failing and restarting until the topic is back, then the signal goes through.
            // Nothing is committed in the meantime, so this is safe.
            throw new DebeziumException(
                    String.format("Smart snapshot: [role=task taskId=%s epoch=%d] Failed to write restart_needed", taskId, epoch), e);
        }
    }

    private void writeCompleted() {
        try {
            snapshotCoordination.writeTaskDone(taskId, epoch);
        }
        catch (Exception e) {
            // can't record completion, the monitor would never downscale; fail so the task retries
            throw new DebeziumException(
                    String.format("Smart snapshot: [role=task taskId=%s epoch=%d] Failed to write completion", taskId, epoch), e);
        }
    }
}
