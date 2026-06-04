/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;

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
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.schema.DatabaseSchema;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.util.LoggingContext;
import io.debezium.snapshot.SnapshotterService;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order. Extends the base
 * {@link ChangeEventSourceCoordinator} to support a pre-snapshot catch up streaming phase and
 * multi-task smart snapshot mode.
 */
public class PostgresChangeEventSourceCoordinator extends ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresChangeEventSourceCoordinator.class);

    private final SnapshotterService snapshotterService;
    private final SlotState slotInfo;
    private final boolean smartSnapshotOnly;
    private final boolean isLeader;
    private final SnapshotCoordination snapshotCoordination;
    private final Lsn slotLsn;
    private final String snapshotName;
    private final int epoch;

    public PostgresChangeEventSourceCoordinator(Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                                ErrorHandler errorHandler,
                                                Class<? extends SourceConnector> connectorType,
                                                CommonConnectorConfig connectorConfig,
                                                PostgresChangeEventSourceFactory changeEventSourceFactory,
                                                ChangeEventSourceMetricsFactory<PostgresPartition> changeEventSourceMetricsFactory,
                                                EventDispatcher<PostgresPartition, ?> eventDispatcher, DatabaseSchema<?> schema,
                                                SnapshotterService snapshotterService, SlotState slotInfo,
                                                SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor,
                                                NotificationService<PostgresPartition, PostgresOffsetContext> notificationService,
                                                boolean smartSnapshotOnly,
                                                boolean isLeader,
                                                SnapshotCoordination snapshotCoordination,
                                                Lsn slotLsn,
                                                String snapshotName,
                                                int epoch) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
                changeEventSourceMetricsFactory, eventDispatcher, schema, signalProcessor, notificationService, snapshotterService);
        this.snapshotterService = snapshotterService;
        this.slotInfo = slotInfo;
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
            ChangeEventSourceContext context
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
                coordinationData.put("snapshot_name", snapshotName);
                coordinationData.put("snapshot_completed", false);
                coordinationData.put(PostgresConnector.EPOCH_KEY, epoch);
                snapshotCoordination.writeSharedData(coordinationData);
                LOGGER.info("Smart snapshot leader: wrote coordination data with LSN={}, snapshot_name={}, epoch={}", slotLsn, snapshotName, epoch);
            } catch (Exception e) {
                throw new DebeziumException("Smart snapshot leader: failed to write coordination data", e);
            }
        }

        previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));
        SnapshotResult<PostgresOffsetContext> snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);

        // Leader: update coordination data with snapshot_completed=true
        if (isLeader && snapshotCoordination != null) {
            try {
                Map<String, Object> coordinationData = new HashMap<>();
                coordinationData.put(SourceInfo.LSN_KEY, slotLsn.asLong());
                coordinationData.put("snapshot_name", snapshotName);
                coordinationData.put("snapshot_completed", true);
                coordinationData.put(PostgresConnector.EPOCH_KEY, epoch);
                snapshotCoordination.writeSharedData(coordinationData);
                LOGGER.info("Smart snapshot leader: updated coordination data with snapshot_completed=true");
            } catch (Exception e) {
                throw new DebeziumException("Smart snapshot leader: failed to update coordination data", e);
            }
        }

        LOGGER.info("Smart snapshot: task completed with result {}, entering idle mode", snapshotResult);
    }

    @Override
    protected CatchUpStreamingResult executeCatchUpStreaming(ChangeEventSourceContext context,
                                                             SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> snapshotSource,
                                                             PostgresPartition partition,
                                                             PostgresOffsetContext previousOffset)
            throws InterruptedException {
        if (previousOffset != null && !snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot() && slotInfo != null) {
            try {
                setSnapshotStartLsn((PostgresSnapshotChangeEventSource) snapshotSource,
                        previousOffset);
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to determine catch-up streaming stopping LSN");
            }
            LOGGER.info("Previous connector state exists and will stream events until {} then perform snapshot",
                    previousOffset.getStreamingStoppingLsn());
            streamEvents(context, partition, previousOffset);
            return new CatchUpStreamingResult(true);
        }

        return new CatchUpStreamingResult(false);
    }

    private void setSnapshotStartLsn(PostgresSnapshotChangeEventSource snapshotSource,
                                     PostgresOffsetContext offsetContext)
            throws SQLException {
        snapshotSource.createSnapshotConnection();
        snapshotSource.setSnapshotTransactionIsolationLevel(false);
        snapshotSource.updateOffsetForPreSnapshotCatchUpStreaming(offsetContext);
    }

}
