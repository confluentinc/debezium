/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.HashMap;
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
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Collect;
import io.debezium.util.LoggingContext;

public class PostgresSmartSnapshotChangeEventSourceCoordinator
        extends PostgresChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            PostgresSmartSnapshotChangeEventSourceCoordinator.class);
    private static final int retryCount = 30;

    private final int epoch;
    private final SnapshotCoordination snapshotCoordination;
    private final String taskId;
    private final String serverName;

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
                                                             SnapshotCoordination snapshotCoordination,
                                                             String taskId,
                                                             String serverName) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig,
                changeEventSourceFactory, changeEventSourceMetricsFactory,
                eventDispatcher, schema, snapshotterService, slotInfo,
                signalProcessor, notificationService);
        this.epoch = epoch;
        this.snapshotCoordination = snapshotCoordination;
        this.taskId = taskId;
        this.serverName = serverName;
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
            Integer offsetEpoch = previousOffset.getEpoch();
            if (offsetEpoch != null && !offsetEpoch.equals(epoch)) {
                LOGGER.info("Smart snapshot [task-{}]: epoch mismatch (offset={}, config={}), clearing offset",
                        taskId, offsetEpoch, epoch);
                previousOffsets.resetOffset(partition);
                previousOffset = null;
            }
        }

        if (previousOffset != null) {
            previousOffset.setEpoch(epoch);
        }

        // --- Generic restart detection (DB-agnostic): (epoch, taskId) membership marker ---
        Map<String, String> markerKey = SmartSnapshotConnectorCoordinator.joinMarkerKey(serverName, taskId);
        Integer markerEpoch = SmartSnapshotConnectorCoordinator.readEpoch(snapshotCoordination.read(markerKey));
        if (markerEpoch != null && markerEpoch == epoch) {
            // rejoining epoch which implies that snapshot transaction can't be rejoined signal full restart.
            LOGGER.warn("Smart snapshot [task-{}]: rejoin detected for the epoch {}, signaling restart_needed", taskId, epoch);
            writeRestartNeeded();
            idleUntilRestart(context);
            return;
        }

        // Stale-epoch check: the Connector already moved to a newer epoch, wait for restart.
        Integer savedEpoch = SmartSnapshotConnectorCoordinator.readEpoch(
                snapshotCoordination.read(SmartSnapshotConnectorCoordinator.epochKey(serverName)));
        if (savedEpoch != null && savedEpoch > epoch) {
            LOGGER.warn("Smart snapshot: task-{}, saved epoch {} is greater than current epoch {}, waiting for restart",
                    taskId, savedEpoch, epoch);
            idleUntilRestart(context);
            return;
        }

        // Fresh join — write marker FIRST (before attaching), so any later restart is caught.
        // If this write fails we just fail the task. We do NOT write restart_needed here: that is also a
        // topic write, so it would fail too. And it isn't needed — we haven't attached yet, so the task did
        // nothing. On restart there is no marker, so it starts fresh at the same epoch. Nothing to clean up.
        Map<String, Object> marker = new HashMap<>();
        marker.put(SmartSnapshotConnectorCoordinator.EPOCH_KEY, epoch);
        try {
            snapshotCoordination.write(markerKey, marker);
        }
        catch (Exception e) {
            throw new DebeziumException(
                    String.format("Smart snapshot [task-%s]: failed to write join marker for the epoch %d", taskId, epoch), e);
        }

        // Read snapshot_name + LSN from coordination topic
        Map<String, String> sharedPartition = Collect.hashMapOf("server", serverName);
        String snapshotName = null;
        String slotLsnStr = null;
        for (int attempt = 0; attempt < retryCount; attempt++) {
            Map<String, Object> coordData = snapshotCoordination.read(sharedPartition);
            if (coordData != null
                    && coordData.get(SmartSnapshotConnectorCoordinator.SNAPSHOT_NAME_KEY) != null) {
                Integer coordEpoch = SmartSnapshotConnectorCoordinator.readEpoch(coordData);
                if (coordEpoch != null && coordEpoch == epoch) {
                    snapshotName = (String) coordData.get(
                            SmartSnapshotConnectorCoordinator.SNAPSHOT_NAME_KEY);
                    slotLsnStr = String.valueOf(coordData.get(
                            SmartSnapshotConnectorCoordinator.SLOT_LSN_KEY));
                    break;
                }
            }
            LOGGER.info("Smart snapshot [task-{}]: waiting for snapshot preparation (attempt {}/30)", taskId, attempt + 1);
            Thread.sleep(2000);
        }
        if (snapshotName == null) {
            throw new DebeziumException(String.format("Smart snapshot [task-%s]: Timed out waiting for snapshot preparation", taskId));
        }

        LOGGER.info("Smart snapshot [task-{}]: got snapshot='{}', LSN={}, executing snapshot-only, epoch={}",
                taskId, snapshotName, slotLsnStr, epoch);

        // Set snapshot name, LSN, and coordination on the source
        PostgresSmartSnapshotChangeEventSource smartSource = (PostgresSmartSnapshotChangeEventSource) snapshotSource;
        smartSource.setSmartSnapshotName(snapshotName);
        smartSource.setSmartSnapshotLsn(Lsn.valueOf(slotLsnStr));
        smartSource.setSnapshotCoordination(snapshotCoordination, epoch);

        try {
            SnapshotResult<PostgresOffsetContext> snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);
            LOGGER.info("Smart snapshot [task-{}]: snapshot completed status={}", taskId, snapshotResult.getStatus());
        }
        catch (InterruptedException e) {
            throw e; // shutdown — not a snapshot failure, do not signal restart
        }
        catch (Exception e) {
            // A real snapshot failure (snapshot gone / SET TRANSACTION SNAPSHOT failed / read error).
            // Here we DO write restart_needed: the task already attached and may have emitted partial data,
            // so the epoch must bump to throw that work away. The topic is likely still up (the failure was
            // in the snapshot, not the write), so the signal should go through and the monitor acts on its
            // next poll. If the write also fails, writeRestartNeeded throws and the marker handles it on restart.
            LOGGER.warn("Smart snapshot [task-{}]: snapshot failed for the epoch {}, signaling restart_needed", taskId, epoch, e);
            writeRestartNeeded();
            throw e instanceof RuntimeException ? (RuntimeException) e
                    : new DebeziumException(String.format("Smart snapshot [task-%s]: snapshot failed", taskId), e);
        }

        // transaction_started signal already sent from lockTablesForSchemaSnapshot()
        // No streaming. Task idles until monitor detects completion.
    }

    private void writeRestartNeeded() {
        try {
            Map<String, Object> data = new HashMap<>();
            data.put(SmartSnapshotConnectorCoordinator.RESTART_NEEDED_KEY, true);
            data.put(SmartSnapshotConnectorCoordinator.EPOCH_KEY, epoch);
            snapshotCoordination.write(SmartSnapshotConnectorCoordinator.taskSignalKey(serverName, taskId), data);
        }
        catch (Exception e) {
            // The topic write failed, so we cannot signal a restart. Just fail the task.
            // On restart the marker is still there, so it tries to signal again. If the topic is still
            // down it keeps failing and restarting until the topic is back, then the signal goes through.
            // Nothing is committed in the meantime, so this is safe.
            throw new DebeziumException(
                    String.format("Smart snapshot [task-%s]: failed to write restart_needed for the epoch %d", taskId, epoch), e);
        }
    }

    private void idleUntilRestart(ChangeEventSourceContext context) throws InterruptedException {
        // Task did not snapshot this epoch; idle until the Connector restarts us at a new epoch.
        while (context.isRunning()) {
            Thread.sleep(5_000);
        }
    }
}
