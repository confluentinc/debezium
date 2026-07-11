/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;
import io.debezium.util.Threads;

public class PostgresSmartSnapshotChangeEventSourceCoordinator
        extends PostgresChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            PostgresSmartSnapshotChangeEventSourceCoordinator.class);
    private static final int SNAPSHOT_INFO_POLL_INTERVAL_MS = 10_000;

    private final int epoch;
    private final SnapshotCoordinationFacade snapshotCoordination;
    private final String taskId;
    // How long to wait for the leader to publish the snapshot info before failing this task.
    private final long snapshotInfoWaitTimeoutMs;

    public PostgresSmartSnapshotChangeEventSourceCoordinator(
                                                             Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                                             ErrorHandler errorHandler,
                                                             Class<? extends SourceConnector> connectorType,
                                                             CommonConnectorConfig connectorConfig,
                                                             PostgresChangeEventSourceFactory changeEventSourceFactory,
                                                             ChangeEventSourceMetricsFactory<PostgresPartition> changeEventSourceMetricsFactory,
                                                             EventDispatcher<PostgresPartition, ?> eventDispatcher,
                                                             DatabaseSchema<?> schema,
                                                             SnapshotterService snapshotterService,
                                                             SlotState slotInfo,
                                                             SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor,
                                                             NotificationService<PostgresPartition, PostgresOffsetContext> notificationService,
                                                             int epoch,
                                                             SnapshotCoordinationFacade snapshotCoordination,
                                                             String taskId) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig,
                changeEventSourceFactory, changeEventSourceMetricsFactory,
                eventDispatcher, schema, snapshotterService, slotInfo,
                signalProcessor, notificationService);
        this.epoch = epoch;
        this.snapshotCoordination = snapshotCoordination;
        this.taskId = taskId;
        this.snapshotInfoWaitTimeoutMs = connectorConfig.getSmartSnapshotTaskSnapshotInfoWaitTimeoutMs();
    }

    /**
     * Catch-up streaming is always OFF for a smart snapshot task.
     *
     * <p>What catch-up streaming does (in the normal, non-smart connector): when the connector restarts in a
     * schema-recovery mode (snapshot.mode = {@code recovery} or {@code schema_only_recovery}) and it already has
     * a saved position and a live replication slot, it first streams the changes from that saved position up to
     * "now", then re-takes the schema snapshot. This fills the gap so no changes are missed while the schema is
     * being rebuilt. It only turns on for those two recovery modes.
     *
     * <p>Why a smart snapshot task never needs it:
     * <ul>
     *   <li>A smart snapshot task only takes a data snapshot of its slice of tables. It never streams. Streaming
     *       happens later, on a single task, after all snapshot tasks finish and the connector scales back down.</li>
     *   <li>Smart snapshot is used for the first data snapshot ({@code initial} / {@code when_needed}), not for the
     *       recovery modes that catch-up streaming exists for.</li>
     *   <li>Changes made while the snapshot runs are not lost anyway: the leader creates the replication slot before
     *       the snapshot starts, so the slot holds on to those changes. When streaming begins after downscale it
     *       resumes from that slot position and replays them. So the gap is already covered, without catch-up
     *       streaming.</li>
     * </ul>
     *
     * <p>Today catch-up streaming is already off here only by accident: this task is built with a {@code null}
     * slot, and the base check needs a non-null slot to run. This override makes it off on purpose, so a later
     * change that starts passing a real slot can't silently turn catch-up streaming on inside a snapshot-only task.
     */
    @Override
    protected CatchUpStreamingResult executeCatchUpStreaming(ChangeEventSourceContext context,
                                                             SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> snapshotSource,
                                                             PostgresPartition partition,
                                                             PostgresOffsetContext previousOffset) {
        return new CatchUpStreamingResult(false);
    }

    @Override
    protected void executeChangeEventSources(
                                             CdcSourceTaskContext taskContext,
                                             SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> snapshotSource,
                                             Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                             AtomicReference<LoggingContext.PreviousContext> previousLogContext,
                                             ChangeEventSourceContext context)
            throws InterruptedException {

        snapshotCoordination.start();

        PostgresPartition partition = previousOffsets.getTheOnlyPartition();
        previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));

        PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        // Epoch mismatch with previous offset → stale from previous round, clear it
        // todo add note on when can it occur
        if (previousOffset != null) {
            // todo test this branch
            Integer offsetEpoch = previousOffset.getEpoch();
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

        // Read snapshot_name + LSN from coordination topic. Wait up to snapshotInfoWaitTimeoutMs, which must be
        // larger than the leader's join-wait + prepare time so we do not give up before the snapshot is published.
        String snapshotName = null;
        String slotLsnStr = null;
        List<TableId> tableSubset = null;
        // The loop is interrupt-aware via metronome.pause() below, which throws InterruptedException (propagated by
        // this method) if the task is being stopped.
        Threads.Timer timer = Threads.timer(Clock.SYSTEM, Duration.ofMillis(snapshotInfoWaitTimeoutMs));
        Metronome metronome = Metronome.parker(Duration.ofMillis(SNAPSHOT_INFO_POLL_INTERVAL_MS), Clock.SYSTEM);
        int attempt = 0;
        while (!timer.expired()) {
            Map<String, Object> snapshotInfo = snapshotCoordination.readSnapshotInfo();
            if (snapshotInfo != null
                    && snapshotInfo.get(SnapshotCoordinationFacade.SNAPSHOT_NAME) != null) {
                Integer snapshotInfoEpoch = SnapshotCoordinationFacade.epochOf(snapshotInfo);
                if (snapshotInfoEpoch != null) {
                    if (snapshotInfoEpoch != epoch) {
                        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Received snapshot info is for a different epoch. receivedEpoch={} currentEpoch={}",
                                taskId, epoch, snapshotInfoEpoch, epoch);
                    }
                    else {
                        snapshotName = (String) snapshotInfo.get(
                                SnapshotCoordinationFacade.SNAPSHOT_NAME);
                        slotLsnStr = String.valueOf(snapshotInfo.get(
                                SnapshotCoordinationFacade.CONSISTENT_POINT));
                        Object taskTableAssignment = SnapshotCoordinationFacade.assignmentForTask(
                                snapshotInfo.get(SnapshotCoordinationFacade.ASSIGNMENTS), Integer.parseInt(taskId));
                        tableSubset = SnapshotCoordinationFacade.parseTablesPostgres(taskTableAssignment);
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

            LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Waiting for snapshot preparation (attempt {}, timeout {}ms)",
                    taskId, epoch, ++attempt, snapshotInfoWaitTimeoutMs);
            metronome.pause();
        }
        if (snapshotName == null) {
            throw new DebeziumException(String.format("Smart snapshot: [role=task taskId=%s epoch=%d] Timed out waiting for snapshot preparation", taskId, epoch));
        }

        // todo should we log each tableId separately?
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Read snapshot info, executing snapshot-only. snapshot={}, LSN={}",
                taskId, epoch, snapshotName, slotLsnStr);

        // Set snapshot name, LSN, and coordination on the source
        PostgresSmartSnapshotChangeEventSource smartSource = (PostgresSmartSnapshotChangeEventSource) snapshotSource;
        // this was captured by the background thread on leader task
        smartSource.setSnapshotCoordination(epoch, snapshotName, Lsn.valueOf(slotLsnStr), tableSubset, snapshotCoordination);

        try {
            SnapshotResult<PostgresOffsetContext> snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);
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
            // producer call can throw a wrapped exception (PSQLException, ClosedByInterruptException,
            // KafkaException) after the interrupt flag was set. Those land here, not in the block above.
            // Treat them like the interrupt path: the task is stopping. On the next start the rejoin path handles
            // cleanup (if it had started its transaction it signals restart; otherwise it re-runs cleanly), and
            // writeRestartNeeded() is a blocking Kafka write that would likely fail under interrupt anyway.
            if (Thread.currentThread().isInterrupted()) {
                LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Interrupted during snapshot (surfaced as {}), exiting gracefully",
                        taskId, epoch, e.getClass().getSimpleName(), e);
                return;
            }

            // A real snapshot failure (snapshot gone / SET TRANSACTION SNAPSHOT failed / read error).
            // Here we DO write restart_needed: the task already attached and may have emitted partial data,
            // so the epoch must bump to throw that work away. The topic is likely still up (the failure was
            // in the snapshot, not the write), so the signal should go through and the monitor acts on its
            // next poll. If the write also fails, writeRestartNeeded throws and the marker handles it on restart.
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
