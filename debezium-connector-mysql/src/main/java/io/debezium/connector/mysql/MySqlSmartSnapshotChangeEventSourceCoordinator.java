/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.List;

import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.config.CommonConnectorConfig;
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
 * MySQL task-side coordinator for a smart snapshot data task. The orchestration lives in
 * {@link AbstractSmartSnapshotChangeEventSourceCoordinator}; this class supplies the MySQL-specific hooks:
 * reading the epoch off a saved offset, and configuring the MySQL smart snapshot source with the decoded
 * binlog position {@code P} (file/pos/gtids) and this task's table slice.
 */
public class MySqlSmartSnapshotChangeEventSourceCoordinator
        extends AbstractSmartSnapshotChangeEventSourceCoordinator<MySqlPartition, MySqlOffsetContext> {

    public MySqlSmartSnapshotChangeEventSourceCoordinator(
                                                          Offsets<MySqlPartition, MySqlOffsetContext> previousOffsets,
                                                          ErrorHandler errorHandler,
                                                          Class<? extends SourceConnector> connectorType,
                                                          CommonConnectorConfig connectorConfig,
                                                          MySqlChangeEventSourceFactory changeEventSourceFactory,
                                                          ChangeEventSourceMetricsFactory<MySqlPartition> changeEventSourceMetricsFactory,
                                                          EventDispatcher<MySqlPartition, ?> eventDispatcher,
                                                          DatabaseSchema<?> schema,
                                                          SnapshotterService snapshotterService,
                                                          SignalProcessor<MySqlPartition, MySqlOffsetContext> signalProcessor,
                                                          NotificationService<MySqlPartition, MySqlOffsetContext> notificationService,
                                                          int epoch,
                                                          SnapshotCoordinationFacade snapshotCoordination,
                                                          String taskId) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig,
                changeEventSourceFactory, changeEventSourceMetricsFactory,
                eventDispatcher, schema, signalProcessor, notificationService, snapshotterService,
                epoch, snapshotCoordination, taskId);
    }

    @Override
    protected Integer epochOf(MySqlOffsetContext offset) {
        return offset.getEpoch();
    }

    @Override
    protected void configureSmartSource(SnapshotChangeEventSource<MySqlPartition, MySqlOffsetContext> snapshotSource,
                                        int epoch,
                                        String snapshotName,
                                        String consistentPoint,
                                        Object assignmentForTask,
                                        SnapshotCoordinationFacade snapshotCoordination) {
        // MySQL identifiers are catalog.table, so parse the assignment with useCatalogScoped=true.
        List<TableId> tableSubset = SmartSnapshotTableAssignments.parseTables(assignmentForTask, true);
        // P is published as "file:pos:gtids"; the gtids segment may itself contain ':' (uuid:range), so keep the
        // split to three parts.
        String[] p = consistentPoint.split(":", 3);
        String binlogFile = p[0];
        long binlogPos = Long.parseLong(p[1]);
        String gtidSet = (p.length > 2 && !p[2].isEmpty()) ? p[2] : null;

        MySqlSmartSnapshotChangeEventSource smartSource = (MySqlSmartSnapshotChangeEventSource) snapshotSource;
        smartSource.setSmartSnapshot(epoch, binlogFile, binlogPos, gtidSet, tableSubset, snapshotCoordination);
    }
}
