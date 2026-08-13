/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.debezium.bean.StandardBeanNames;
import io.debezium.connector.common.DebeziumHeaderProducerProvider;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.processors.PostProcessorRegistryServiceProvider;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotLockProvider;
import io.debezium.snapshot.SnapshotQueryProvider;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.snapshot.SnapshotterServiceProvider;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Table discovery for smart snapshot, run by the task-0 leader thread. Reuses the real single-task discovery
 * chain ({@code getSnapshottingTask -> prepare -> connectionCreated -> determineCapturedTables}) inherited from
 * {@link SqlServerSnapshotChangeEventSource} rather than a hand-rolled filter, so it respects
 * {@code snapshot.include.collection.list}, table filters, and signal-table inclusion identically to single-task
 * mode.
 *
 * <p>Of that chain's dependencies, only {@link SnapshotterService} is actually invoked, and it's
 * standalone-constructible from the connector config ({@link #buildSnapshotterService}); the schema, dispatcher,
 * clock, progress listener, and notification service are never touched by this chain and so are passed
 * {@code null}/no-op (notably, a real schema is not built -- constructing one would start the schema-history
 * backend).
 */
class SqlServerLeaderSchemaSource extends SqlServerSnapshotChangeEventSource {

    static class DiscoveryResult {
        final List<TableId> capturedTables;
        final List<TableId> uncapturedEligibleTables;

        DiscoveryResult(List<TableId> capturedTables, List<TableId> uncapturedEligibleTables) {
            this.capturedTables = capturedTables;
            this.uncapturedEligibleTables = uncapturedEligibleTables;
        }
    }

    SqlServerLeaderSchemaSource(SqlServerConnectorConfig connectorConfig,
                                MainConnectionProvidingConnectionFactory<SqlServerConnection> connectionFactory,
                                SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, null, null, Clock.system(),
                SnapshotProgressListener.NO_OP(), null, snapshotterService);
    }

    /**
     * Runs the discovery pipeline for one database and returns both the data-captured set (the round's
     * shard-splitting universe) and the eligible-but-uncaptured leftover set (only non-empty under
     * {@code store.only.captured.tables.ddl=false}).
     */
    @SuppressWarnings("unchecked")
    DiscoveryResult discover(SqlServerPartition partition) throws Exception {
        SnapshottingTask task = getSnapshottingTask(partition, null);
        RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> ctx = (RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext>) prepare(
                partition, false);
        connectionCreated(ctx);
        determineCapturedTables(ctx, getDataCollectionPattern(task.getDataCollections()), task);

        List<TableId> captured = new ArrayList<>(ctx.capturedTables);
        List<TableId> uncaptured = ctx.capturedSchemaTables.stream()
                .filter(tableId -> !ctx.capturedTables.contains(tableId))
                .collect(Collectors.toList());
        return new DiscoveryResult(captured, uncaptured);
    }

    /**
     * Standalone-constructs a {@link SnapshotterService} from just the connector config, registering the same
     * providers {@code BaseSourceTask.registerServiceProviders()} does. The {@code DATABASE_SCHEMA} bean is a
     * minimal stub: the Snapshotter's {@code shouldSnapshotSchema()} only ever calls {@code isHistorized()} on
     * it, so a full schema (which would start the schema-history backend) isn't needed.
     */
    static SnapshotterService buildSnapshotterService(SqlServerConnectorConfig connectorConfig) {
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, new HistorizedSchemaStub());
        connectorConfig.getServiceRegistry().registerServiceProvider(new PostProcessorRegistryServiceProvider());
        connectorConfig.getServiceRegistry().registerServiceProvider(new SnapshotLockProvider());
        connectorConfig.getServiceRegistry().registerServiceProvider(new SnapshotQueryProvider());
        connectorConfig.getServiceRegistry().registerServiceProvider(new SnapshotterServiceProvider());
        connectorConfig.getServiceRegistry().registerServiceProvider(new DebeziumHeaderProducerProvider());
        return connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);
    }

    /** Answers only {@link #isHistorized()}; nothing in the discovery call chain calls anything else. */
    private static final class HistorizedSchemaStub implements DatabaseSchema<DataCollectionId> {
        @Override
        public DataCollectionSchema schemaFor(DataCollectionId id) {
            throw new UnsupportedOperationException("not used during smart-snapshot table discovery");
        }

        @Override
        public boolean tableInformationComplete() {
            throw new UnsupportedOperationException("not used during smart-snapshot table discovery");
        }

        @Override
        public boolean isHistorized() {
            return true;
        }

        @Override
        public void close() {
        }
    }
}
