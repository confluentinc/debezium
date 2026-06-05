/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.LoggingContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order. Extends the base
 * {@link ChangeEventSourceCoordinator} to support a pre-snapshot catch up streaming phase.
 */
public class PostgresSmartSnapshotChangeEventSourceCoordinator extends PostgresChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSmartSnapshotChangeEventSourceCoordinator.class);

    private final boolean smartSnapshotOnly;
    private final boolean isLeader;
    private final SnapshotCoordination snapshotCoordination;
    private final Lsn slotLsn;
    private final String snapshotName;
    private final int epoch;

    public PostgresSmartSnapshotChangeEventSourceCoordinator(
            Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
            ErrorHandler errorHandler, Class<? extends SourceConnector> connectorType,
            CommonConnectorConfig connectorConfig,
            PostgresChangeEventSourceFactory changeEventSourceFactory,
            ChangeEventSourceMetricsFactory<PostgresPartition> changeEventSourceMetricsFactory,
            EventDispatcher<PostgresPartition, ?> eventDispatcher, DatabaseSchema<?> schema,
            SnapshotterService snapshotterService,
            SlotState slotInfo,
            SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor,
            NotificationService<PostgresPartition, PostgresOffsetContext> notificationService,
            boolean smartSnapshotOnly,
            boolean isLeader,
            SnapshotCoordination snapshotCoordination,
            Lsn slotLsn,
            String snapshotName,
            int epoch) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory, changeEventSourceMetricsFactory, eventDispatcher, schema, snapshotterService, slotInfo, signalProcessor, notificationService);
        this.smartSnapshotOnly = smartSnapshotOnly;
        this.isLeader = isLeader;
        this.snapshotCoordination = snapshotCoordination;
        this.slotLsn = slotLsn;
        this.snapshotName = snapshotName;
        this.epoch = epoch;
    }

    @Override
    protected void executeChangeEventSources(
            CdcSourceTaskContext taskContext,
            SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> snapshotSource,
            Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
            AtomicReference<LoggingContext.PreviousContext> previousLogContext,
            ChangeEventSource.ChangeEventSourceContext context
    ) throws InterruptedException {

        if (!smartSnapshotOnly) {
            super.executeChangeEventSources(taskContext, snapshotSource, previousOffsets, previousLogContext, context);
            return;
        }

        // Multi-task smart snapshot mode: snapshot only, then idle
        final PostgresPartition partition = previousOffsets.getTheOnlyPartition();
        final PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        // Leader: write coordination data before snapshot
        if (isLeader && snapshotCoordination != null) {
            try {
                Map<String, Object> coordinationData = new HashMap<>();
                coordinationData.put(SourceInfo.LSN_KEY, slotLsn.asLong());
                coordinationData.put(PostgresConnector.SNAPSHOT_NAME_KEY, snapshotName);
                coordinationData.put(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY, false);
                coordinationData.put(PostgresConnector.EPOCH_KEY, epoch);
                snapshotCoordination.writeSharedData(coordinationData);
                LOGGER.info("Smart snapshot leader: wrote coordination data with LSN={}, snapshot_name={}, epoch={}", slotLsn, snapshotName, epoch);
            } catch (Exception e) {
                throw new DebeziumException("Smart snapshot leader: failed to write coordination data", e);
            }
        }

        SnapshotResult<PostgresOffsetContext> snapshotResult;

        try {
            previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));
            snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);
        }
        catch (Exception e) {
            // Check if this is a stale snapshot error (SET TRANSACTION SNAPSHOT failed)
            if (isStaleSnapshotError(e) && snapshotCoordination != null) {
                LOGGER.warn("Smart snapshot: snapshot failed due to stale snapshot, writing restart_required=true to trigger reconfiguration.", e);

                try {
                    Map<String, Object> existingData = snapshotCoordination.readSharedData();
                    if (existingData != null) {
                        Map<String, Object> updated = new HashMap<>(existingData);
                        updated.put(PostgresConnector.RESTART_KEY, true);
                        snapshotCoordination.writeSharedData(updated);
                        LOGGER.info("Smart snapshot: wrote restart_required=true to coordination data");
                    }
                }
                catch (Exception coordinatorException) {
                    LOGGER.warn("Smart snapshot: failed to write restart_required to coordination data", coordinatorException);
                }
            }
            throw e;
        }

        // Leader: update coordination data with snapshot_completed=true
        if (isLeader && snapshotCoordination != null) {
            try {
                Map<String, Object> coordinationData = new HashMap<>();
                coordinationData.put(SourceInfo.LSN_KEY, slotLsn.asLong());
                coordinationData.put(PostgresConnector.SNAPSHOT_NAME_KEY, snapshotName);
                coordinationData.put(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY, true);
                coordinationData.put(PostgresConnector.EPOCH_KEY, epoch);
                snapshotCoordination.writeSharedData(coordinationData);
                LOGGER.info("Smart snapshot leader: updated coordination data with snapshot_completed=true");
            } catch (Exception e) {
                throw new DebeziumException("Smart snapshot leader: failed to update coordination data", e);
            }
        }

        LOGGER.info("Smart snapshot: task completed with result {}, entering idle mode", snapshotResult);
    }

    private boolean isStaleSnapshotError(Exception e) {
        if (!smartSnapshotOnly) {
            return false;
        }
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof SQLException) {
                String msg = cause.getMessage();
                if (msg != null && msg.contains("snapshot") && msg.contains("does not exist")) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }
}
