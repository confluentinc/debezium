/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.List;

import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.AbstractSmartSnapshotChangeEventSourceCoordinator;
import io.debezium.pipeline.source.snapshot.SmartSnapshotTableAssignments;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;

/**
 * Postgres task-side coordinator for a smart snapshot data task. All the orchestration lives in
 * {@link AbstractSmartSnapshotChangeEventSourceCoordinator}; this class only supplies the two Postgres-specific
 * hooks: reading the epoch off a saved offset, and configuring the Postgres smart snapshot source with the
 * exported snapshot name, the slot LSN, and this task's table slice.
 */
public class PostgresSmartSnapshotChangeEventSourceCoordinator
        extends AbstractSmartSnapshotChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> {

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
                                                             SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor,
                                                             NotificationService<PostgresPartition, PostgresOffsetContext> notificationService,
                                                             int epoch,
                                                             SnapshotCoordinationFacade snapshotCoordination,
                                                             String taskId) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig,
                changeEventSourceFactory, changeEventSourceMetricsFactory,
                eventDispatcher, schema, signalProcessor, notificationService, snapshotterService,
                epoch, snapshotCoordination, taskId);
    }

    @Override
    protected Integer epochOf(PostgresOffsetContext offset) {
        return offset.getEpoch();
    }

    @Override
    protected void configureSmartSource(SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> snapshotSource,
                                        int epoch,
                                        String snapshotName,
                                        String consistentPoint,
                                        Object assignmentForTask,
                                        SnapshotCoordinationFacade snapshotCoordination) {
        // Postgres identifiers are schema.table, so parse the assignment with useCatalogScoped=false.
        List<TableId> tableSubset = SmartSnapshotTableAssignments.parseTables(assignmentForTask, false);
        PostgresSmartSnapshotChangeEventSource smartSource = (PostgresSmartSnapshotChangeEventSource) snapshotSource;
        // this was captured by the background thread on the leader task
        smartSource.setSnapshotCoordination(epoch, snapshotName, Lsn.valueOf(consistentPoint), tableSubset, snapshotCoordination);
    }
}
