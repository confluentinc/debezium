/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

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
import io.debezium.util.LoggingContext;

public class PostgresSmartSnapshotChangeEventSourceCoordinator
        extends PostgresChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            PostgresSmartSnapshotChangeEventSourceCoordinator.class);
    private static final int SNAPSHOT_INFO_POLL_RETRY_COUNT = 30;
    private static final int IDLE_WAIT_COUNT = 30;
    private static final int IDLE_DELAY_MS = 10_000;

    private final int epoch;
    private final SnapshotCoordinationFacade snapshotCoordination;
    private final String taskId;

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
        // NOT a crash. Do not treat it as a rejoin; idle and let the connector downscale.
        boolean done = snapshotCoordination.isTaskDone(taskId, epoch);
        if (done) {
            LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Already completed, idling", taskId, epoch);
            idleUntilRestart(context);
            return;
        }

        // Generic restart detection (DB-agnostic): (epoch, taskId) membership marker
        Integer markerEpoch = snapshotCoordination.readTaskJoinEpoch(taskId);
        if (markerEpoch != null && markerEpoch == epoch) {
            // rejoining epoch which implies that snapshot transaction can't be rejoined signal full restart.
            LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Rejoin detected, signaling `restart_needed`", taskId, epoch);
            writeRestartNeeded();
            idleUntilRestart(context);
            return;
        }

        // Stale-epoch check: the Connector already moved to a newer epoch, wait for restart.
        Integer savedEpoch = snapshotCoordination.readEpoch();
        if (savedEpoch != null && savedEpoch > epoch) {
            LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Saved epoch is greater than current epoch, waiting for restart. savedEpoch={} currentEpoch={}",
                    taskId, epoch, savedEpoch, epoch);
            idleUntilRestart(context);
            return;
        }

        // Fresh join — write marker FIRST (before attaching), so any later restart is caught.
        // If this write fails we just fail the task. We do NOT write restart_needed here: that is also a
        // topic write, so it would fail too. And it isn't needed — we haven't attached yet, so the task did
        // nothing. On restart there is no marker, so it starts fresh at the same epoch. Nothing to clean up.

        // if the write fails let the task fail
        snapshotCoordination.writeTaskJoin(taskId, epoch);

        // Read snapshot_name + LSN from coordination topic
        String snapshotName = null;
        String slotLsnStr = null;
        List<TableId> tableSubset = null;
        for (int attempt = 0; attempt < SNAPSHOT_INFO_POLL_RETRY_COUNT; attempt++) {
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
            LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Waiting for snapshot preparation (attempt {}/30)", taskId, epoch, attempt + 1);
            Thread.sleep(IDLE_DELAY_MS);
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
            // Treat them like the interrupt path: the task is stopping, the join marker already guarantees
            // the rejoin path signals restart_needed on the next start, and writeRestartNeeded() is a blocking
            // Kafka write that would likely fail under interrupt anyway.
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

        // todo check if this is really required?
        // or a better way to sleep
        idleUntilRestart(context);
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

    private void idleUntilRestart(ChangeEventSourceContext context) throws InterruptedException {
        // can't wait forever for restart, what if the monitor thread on connector dies?
        // wait for 5 mins
        for (int i = 0; i < IDLE_WAIT_COUNT; i++) {
            if (context.isRunning()) {
                Thread.sleep(IDLE_DELAY_MS);
                if (i % 3 == 0) { // log every 30 second
                    LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Idling", taskId, epoch);
                }
            }
            else {
                return;
            }
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
