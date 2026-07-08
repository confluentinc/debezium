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
 * Connector-side table discovery for smart snapshot (design §11.0/§15.7), reusing the *real* single-task
 * discovery pipeline (an unmodified {@code getSnapshottingTask -> prepare -> connectionCreated ->
 * determineCapturedTables} call chain inherited from {@link SqlServerSnapshotChangeEventSource}) instead of
 * a hand-rolled filter -- so smart snapshot respects {@code snapshot.include.collection.list} and forced
 * signal-table inclusion exactly like single-task mode does, not an approximation of it.
 *
 * <p>Mirrors {@code PostgresSmartSnapshotLifecycleManager.PostgresLeaderSchemaSource} in spirit (call the real
 * method, don't reimplement it) but not in placement: Postgres's leader runs from task-0, reusing that task's
 * already-built {@code EventDispatcher}/{@code PostgresSchema}/etc. This design cannot do that -- table-driven
 * allocation needs the table count *before* {@code taskConfigs()} decides how many tasks to create, and task-0
 * doesn't exist yet at that point -- so this runs at the Connector instead. Verified by reading every method in
 * the call chain: only {@link SnapshotterService} is a real, actively-used dependency (`getSnapshottingTask()`
 * calls `.getSnapshotter()`), and it's cheaply standalone-constructible from just the connector config (see
 * {@link #buildSnapshotterService}) -- no task-level bean required. `Clock`, {@link SnapshotProgressListener},
 * {@link NotificationService}, and the schema/dispatcher fields are stored-but-never-invoked by this exact
 * chain, so they're `null`/no-op here rather than built-and-discarded real instances (building a real
 * {@code SqlServerDatabaseSchema} has a side effect -- it starts the schema-history backend -- even though
 * nothing in this call chain ever uses it).
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
     * Runs the real discovery pipeline (design §15.7) for one database and returns both the data-captured
     * set (what becomes the round's shard-splitting universe) and the eligible-but-uncaptured leftover set
     * (design §6.2, only non-empty under {@code store.only.captured.tables.ddl=false}).
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
     * Standalone-constructs a real {@link SnapshotterService} from just the connector config -- no task-level
     * bean required. Mirrors exactly what {@code BaseSourceTask.registerServiceProviders()} registers; the
     * service only ever looks up the connector-config bean it's given here.
     *
     * <p>One refinement versus the original design §15.7 investigation: {@code getSnapshottingTask()}'s
     * {@code Snapshotter.shouldSnapshotSchema()} (e.g. {@code InitialSnapshotter}) looks up a
     * {@code DATABASE_SCHEMA} bean and calls {@code isHistorized()} on it -- a real dependency this class's
     * javadoc didn't originally account for. A full {@link SqlServerDatabaseSchema} still isn't needed (and
     * still isn't built, avoiding its schema-history-start side effect): {@code isHistorized()} is the only
     * method ever called on it in this path, so a minimal stub answering `true` (SQL Server's schema is
     * always historized) is registered instead.
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
