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
        if (previousOffset != null) {
            // todo test this branch
            Integer offsetEpoch = previousOffset.getEpoch();
            if (offsetEpoch != null && !offsetEpoch.equals(epoch)) {
                LOGGER.info("Smart snapshot: [task-{}] Epoch mismatch (offsetEpoch {}, configEpoch {}), clearing offset",
                        taskId, offsetEpoch, epoch);
                previousOffsets.resetOffset(partition);
                previousOffset = null;
            }
        }

        // previousOffset here is non-null only if its epoch == this epoch (the epoch-mismatch block above
        // reset it otherwise). If it shows the snapshot already completed, this restart is just the task
        // being bounced AFTER finishing (a reconfiguration or the managed runtime stopping a "done" task)
        // NOT a crash. Do not treat it as a rejoin; idle and let the connector downscale.
        boolean done = snapshotCoordination.isDone(taskId, epoch);
        if (done) {
            LOGGER.info("Smart snapshot: [task-{}] Already completed for the epoch {}, idling", taskId, epoch);
            idleUntilRestart(context);
            return;
        }

        // Generic restart detection (DB-agnostic): (epoch, taskId) membership marker
        Integer markerEpoch = snapshotCoordination.readJoinEpoch(taskId);
        if (markerEpoch != null && markerEpoch == epoch) {
            // rejoining epoch which implies that snapshot transaction can't be rejoined signal full restart.
            LOGGER.warn("Smart snapshot: [task-{}] Rejoin detected for the epoch {}, signaling `restart_needed`", taskId, epoch);
            writeRestartNeeded();
            idleUntilRestart(context);
            return;
        }

        // Stale-epoch check: the Connector already moved to a newer epoch, wait for restart.
        Integer savedEpoch = snapshotCoordination.readEpoch();
        if (savedEpoch != null && savedEpoch > epoch) {
            LOGGER.warn("Smart snapshot: [task-{}] Saved epoch {} is greater than current epoch {}, waiting for restart",
                    taskId, savedEpoch, epoch);
            idleUntilRestart(context);
            return;
        }

        // Fresh join — write marker FIRST (before attaching), so any later restart is caught.
        // If this write fails we just fail the task. We do NOT write restart_needed here: that is also a
        // topic write, so it would fail too. And it isn't needed — we haven't attached yet, so the task did
        // nothing. On restart there is no marker, so it starts fresh at the same epoch. Nothing to clean up.

        // if the write fails let the task fail
        snapshotCoordination.writeJoin(taskId, epoch);

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
                        LOGGER.info("Smart snapshot: [task-{}] received snapshot info is for a different epoch, receivedEpoch {} currentEpoch {}", taskId,
                                snapshotInfoEpoch, epoch);
                    }
                    else {
                        snapshotName = (String) snapshotInfo.get(
                                SnapshotCoordinationFacade.SNAPSHOT_NAME);
                        slotLsnStr = String.valueOf(snapshotInfo.get(
                                SnapshotCoordinationFacade.CONSISTENT_POINT));
                        List<TableId> all = SnapshotCoordinationFacade.parseTablesPostgres(snapshotInfo.get(SnapshotCoordinationFacade.TABLES));
                        int numTasks = ((Number) snapshotInfo.get(SnapshotCoordinationFacade.NUM_TASKS)).intValue();
                        tableSubset = SnapshotCoordinationFacade.tablesForTask(all, Integer.parseInt(taskId), numTasks);
                        break;
                    }
                }
            }
            LOGGER.info("Smart snapshot: [task-{}] waiting for snapshot preparation (attempt {}/30)", taskId, attempt + 1);
            Thread.sleep(IDLE_DELAY_MS);
        }
        if (snapshotName == null) {
            throw new DebeziumException(String.format("Smart snapshot [task-%s]: Timed out waiting for snapshot preparation", taskId));
        }

        // todo should we log each tableId separately?
        LOGGER.info("Smart snapshot: [task-{}] Read snapshot info, snapshot={}, LSN={}, executing snapshot-only, epoch={}",
                taskId, snapshotName, slotLsnStr, epoch);

        // Set snapshot name, LSN, and coordination on the source
        PostgresSmartSnapshotChangeEventSource smartSource = (PostgresSmartSnapshotChangeEventSource) snapshotSource;
        // this was captured by the background thread on leader task
        smartSource.setSnapshotCoordination(epoch, snapshotName, Lsn.valueOf(slotLsnStr), tableSubset, snapshotCoordination);

        try {
            SnapshotResult<PostgresOffsetContext> snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);
            LOGGER.info("Smart snapshot: [task-{}] Snapshot completed status={}", taskId, snapshotResult.getStatus());
        }
        catch (InterruptedException e) {
            LOGGER.warn("Smart snapshot: [task-{}] Interrupted while waiting, exiting gracefully for the epoch {}", taskId, epoch, e);
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            // A real snapshot failure (snapshot gone / SET TRANSACTION SNAPSHOT failed / read error).
            // Here we DO write restart_needed: the task already attached and may have emitted partial data,
            // so the epoch must bump to throw that work away. The topic is likely still up (the failure was
            // in the snapshot, not the write), so the signal should go through and the monitor acts on its
            // next poll. If the write also fails, writeRestartNeeded throws and the marker handles it on restart.

            // todo should we check for interrupted here?
            LOGGER.warn("Smart snapshot: [task-{}] Snapshot failed for the epoch {}, signaling `restart_needed`", taskId, epoch, e);
            writeRestartNeeded();
            throw new DebeziumException(String.format("Smart snapshot: [task-%s] Snapshot failed for the epoch %s, signaling restart_needed", taskId, epoch), e);
        }

        writeCompleted();

        // todo check if this is really required?
        // todo catch interrupt ?
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
                    String.format("Smart snapshot: [task-%s] Failed to write restart_needed for the epoch %d", taskId, epoch), e);
        }
    }

    private void idleUntilRestart(ChangeEventSourceContext context) throws InterruptedException {
        // can't wait forever for restart, what if the monitor thread on connector dies?
        // wait for 5 mins
        for (int i = 0; i < IDLE_WAIT_COUNT; i++) {
            if (context.isRunning()) {
                Thread.sleep(IDLE_DELAY_MS);
                if (i % 3 == 0) { // log every 30 second
                    LOGGER.info("Smart snapshot: [task-{}] is idling for the epoch {}", taskId, epoch);
                }
            }
            else {
                return;
            }
        }
    }

    private void writeCompleted() {
        try {
            snapshotCoordination.writeDone(taskId, epoch);
        }
        catch (Exception e) {
            // can't record completion, the monitor would never downscale; fail so the task retries
            throw new DebeziumException(
                    String.format("Smart snapshot [task-%s]: Failed to write completion for the epoch %d", taskId, epoch), e);
        }
    }
}
