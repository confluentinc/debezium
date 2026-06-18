/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * Multi-task ("smart snapshot") coordinator for SQL Server. Unlike the base coordinator it runs the snapshot
 * <b>only</b> (no streaming): it reads the Connector-prepared consistent position {@code L_db} (and epoch)
 * from the coordination topic, hands it to the {@link SqlServerSmartSnapshotChangeEventSource}, and snapshots
 * this task's table shard. The task then idles until the Connector monitor detects completion of all tasks
 * and downscales to the normal streaming layout.
 */
public class SqlServerSmartSnapshotChangeEventSourceCoordinator extends SqlServerChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSmartSnapshotChangeEventSourceCoordinator.class);
    private static final int RETRY_COUNT = 30;

    private final int epoch;
    private final SnapshotCoordination snapshotCoordination;
    private final String taskId;

    public SqlServerSmartSnapshotChangeEventSourceCoordinator(Offsets<SqlServerPartition, SqlServerOffsetContext> previousOffsets, ErrorHandler errorHandler,
                                                              Class<? extends SourceConnector> connectorType,
                                                              CommonConnectorConfig connectorConfig,
                                                              ChangeEventSourceFactory<SqlServerPartition, SqlServerOffsetContext> changeEventSourceFactory,
                                                              ChangeEventSourceMetricsFactory<SqlServerPartition> changeEventSourceMetricsFactory,
                                                              EventDispatcher<SqlServerPartition, ?> eventDispatcher,
                                                              DatabaseSchema<?> schema,
                                                              Clock clock,
                                                              SignalProcessor<SqlServerPartition, SqlServerOffsetContext> signalProcessor,
                                                              NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService,
                                                              SnapshotterService snapshotterService,
                                                              int epoch,
                                                              SnapshotCoordination snapshotCoordination,
                                                              String taskId) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory, changeEventSourceMetricsFactory,
                eventDispatcher, schema, clock, signalProcessor, notificationService, snapshotterService);
        this.epoch = epoch;
        this.snapshotCoordination = snapshotCoordination;
        this.taskId = taskId;
    }

    @Override
    protected void executeChangeEventSources(CdcSourceTaskContext taskContext, SnapshotChangeEventSource<SqlServerPartition, SqlServerOffsetContext> snapshotSource,
                                             Offsets<SqlServerPartition, SqlServerOffsetContext> previousOffsets,
                                             AtomicReference<LoggingContext.PreviousContext> previousLogContext,
                                             ChangeEventSourceContext context)
            throws InterruptedException {

        snapshotCoordination.start();
        final SqlServerSmartSnapshotChangeEventSource smartSource = (SqlServerSmartSnapshotChangeEventSource) snapshotSource;

        // A smart-snapshot task is DB-exclusive, so there is normally a single partition; iterate defensively.
        for (Map.Entry<SqlServerPartition, SqlServerOffsetContext> entry : previousOffsets) {
            SqlServerPartition partition = entry.getKey();
            SqlServerOffsetContext previousOffset = entry.getValue();
            previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));

            // A per-task offset from a previous coordination round is stale; drop it.
            if (previousOffset != null) {
                Integer offsetEpoch = previousOffset.getEpoch();
                if (offsetEpoch != null && !offsetEpoch.equals(epoch)) {
                    LOGGER.info("Smart snapshot [task-{}]: epoch mismatch (offset={}, config={}), clearing offset", taskId, offsetEpoch, epoch);
                    previousOffsets.resetOffset(partition);
                    previousOffset = null;
                }
            }
            if (previousOffset != null) {
                previousOffset.setEpoch(epoch);
            }

            String lsn = awaitConsistentPosition(partition);
            smartSource.setSmartSnapshotLsn(lsn);
            smartSource.setSnapshotCoordination(snapshotCoordination, epoch);

            LOGGER.info("Smart snapshot [task-{}]: db={}, L_db={}, epoch={}, running snapshot-only",
                    taskId, partition.getDatabaseName(), lsn, epoch);
            SnapshotResult<SqlServerOffsetContext> result = doSnapshot(snapshotSource, context, partition, previousOffset);
            LOGGER.info("Smart snapshot [task-{}]: snapshot completed status={}", taskId, result.getStatus());
        }

        // No streaming. The task idles; the Connector monitor detects completion and triggers downscale.
        LOGGER.info("Smart snapshot [task-{}]: snapshot phase finished, idling until downscale", taskId);
    }

    /**
     * Polls the coordination topic for this database's prepared consistent position (L_db) at the current epoch.
     */
    private String awaitConsistentPosition(SqlServerPartition partition) throws InterruptedException {
        Map<String, String> shared = partition.getSharedSourcePartition();
        for (int attempt = 0; attempt < RETRY_COUNT; attempt++) {
            Map<String, Object> coordData = snapshotCoordination.read(shared);
            if (coordData != null && coordData.get(SmartSnapshotConnectorCoordinator.SLOT_LSN_KEY) != null) {
                Integer coordEpoch = SmartSnapshotConnectorCoordinator.readEpoch(coordData);
                if (coordEpoch != null && coordEpoch == epoch) {
                    return String.valueOf(coordData.get(SmartSnapshotConnectorCoordinator.SLOT_LSN_KEY));
                }
            }
            LOGGER.info("Smart snapshot [task-{}]: waiting for snapshot preparation (attempt {}/{})", taskId, attempt + 1, RETRY_COUNT);
            Thread.sleep(2000);
        }
        throw new DebeziumException("Smart snapshot [task-" + taskId + "]: timed out waiting for snapshot preparation for db="
                + partition.getDatabaseName());
    }
}
