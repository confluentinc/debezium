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

/**
 * Coordinator for multi-task smart snapshot mode. Extends {@link PostgresChangeEventSourceCoordinator}
 * to run snapshot only (no streaming) and coordinate via the offset topic.
 *
 * <p>Overrides {@link #executeChangeEventSources} to:
 * <ul>
 *   <li>Leader: write coordination data (LSN, snapshot_name, epoch) to {@code {"server":"<prefix>"}}
 *       before snapshot, and update {@code snapshot_completed=true} after.</li>
 *   <li>All tasks: run {@code doSnapshot()} then idle, no streaming.</li>
 *   <li>On stale snapshot error ({@code SET TRANSACTION SNAPSHOT} fails):
 *       write {@code restart_required=true} to trigger full reconfiguration via the Connector's monitor thread.</li>
 * </ul>
 *
 * <p>Used only when {@code smart.snapshot=true} and {@code tasks.max > 1}. Single-task mode
 * uses the parent {@link PostgresChangeEventSourceCoordinator} unchanged.
 */
public class PostgresSmartSnapshotChangeEventSourceCoordinator extends PostgresChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSmartSnapshotChangeEventSourceCoordinator.class);

    private final String taskId;
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
                                                             String taskId,
                                                             boolean isLeader,
                                                             SnapshotCoordination snapshotCoordination,
                                                             Lsn slotLsn,
                                                             String snapshotName,
                                                             int epoch) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory, changeEventSourceMetricsFactory, eventDispatcher, schema,
                snapshotterService, slotInfo, signalProcessor, notificationService);
        this.taskId = taskId;
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
                                             ChangeEventSource.ChangeEventSourceContext context)
            throws InterruptedException {
        // smart snapshot mode: snapshot only, then idle
        final PostgresPartition partition = previousOffsets.getTheOnlyPartition();
        final PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        LOGGER.info("Smart snapshot: task-{} entering snapshot phase", taskId);

        // Leader: write coordination data before snapshot
        if (isLeader) {
            try {
                Map<String, Object> coordinationData = new HashMap<>();
                coordinationData.put(SourceInfo.LSN_KEY, slotLsn.asLong());
                coordinationData.put(PostgresConnector.SNAPSHOT_NAME_KEY, snapshotName);
                coordinationData.put(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY, false);
                coordinationData.put(PostgresConnector.EPOCH_KEY, epoch);
                snapshotCoordination.writeSharedData(coordinationData);
                LOGGER.info("Smart snapshot: Leader wrote coordination data with LSN={}, snapshot_name={}, epoch={}", slotLsn, snapshotName, epoch);
            }
            catch (Exception e) {
                throw new DebeziumException("Smart snapshot: Leader failed to write coordination data", e);
            }
        }

        SnapshotResult<PostgresOffsetContext> snapshotResult;

        /** if (previousOffset != null) {
         **   previousOffset.setEpoch(epoch);
        }**/
        try {
            previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));
            snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);

            if (snapshotResult.getOffset() != null) {
                // snapshotResult.getOffset().setEpoch(epoch);
            }
        }
        catch (Exception e) {
            // Check if this is a stale snapshot error (SET TRANSACTION SNAPSHOT failed)
            if (isStaleSnapshotError(e)) {
                LOGGER.warn("Smart snapshot: task-{} snapshot failed due to stale snapshot, writing restart_required=true to trigger reconfiguration.", taskId, e);

                try {
                    Map<String, Object> existingData = snapshotCoordination.readSharedData();
                    if (existingData != null) {
                        Map<String, Object> updated = new HashMap<>(existingData);
                        updated.put(PostgresConnector.RESTART_KEY, true);
                        snapshotCoordination.writeSharedData(updated);
                        LOGGER.info("Smart snapshot: task-{} wrote restart_required=true to coordination data", taskId);
                    }
                }
                catch (Exception coordinatorException) {
                    LOGGER.warn("Smart snapshot: task-{} failed to write restart_required to coordination data", taskId, coordinatorException);
                }
            }
            throw e;
        }

        LOGGER.info("Smart snapshot: task-{} completed with result {}, entering idle mode", taskId, snapshotResult);
    }

    /**
     * Checks whether the exception was caused by a stale Postgres snapshot, specifically,
     * a {@code SET TRANSACTION SNAPSHOT '<name>'} that failed because the exporting
     * transaction is no longer alive (e.g., leader crashed, replication connection closed).
     * Postgres reports this as an error containing "snapshot" and "does not exist".
     */
    private boolean isStaleSnapshotError(Exception e) {
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
