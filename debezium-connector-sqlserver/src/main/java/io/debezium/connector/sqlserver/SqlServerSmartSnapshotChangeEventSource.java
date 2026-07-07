/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.re2j.Pattern;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Sharded snapshot task change-event source for the smart-snapshot {@code repeatable_read} design: reads
 * {@code L_db} and its own table shard from coordination (injected by
 * {@link SqlServerSmartSnapshotChangeEventSourceCoordinator} once it has polled the shared snapshot-info
 * record and computed this task's shard via {@code SnapshotCoordinationFacade.tablesForTask}) instead of
 * discovering tables/capturing its own anchor itself (design §11.0 -- only the Connector captures the
 * anchor). Overriding {@code determineCapturedTables} to assign the pre-computed shard to both
 * {@code capturedTables} and {@code capturedSchemaTables} means every task -- including the one whose shard
 * happens to land at index 0 -- dispatches schema-history CREATE TABLEs for only its own shard, from the
 * same live read it uses for its own data (design §6.2): the union across all shards is what makes
 * schema-history complete, not any single task dispatching everything.
 *
 * <p>Schema *locking* is intentionally left untouched: the inherited {@code lockTablesForSchemaSnapshot}/
 * {@code releaseSchemaSnapshotLocks} already do the per-table {@code TABLOCKX}-then-release dance that this
 * design wants (§6.1) -- there is no write barrier to skip, unlike the {@code snapshot}-isolation design.
 */
public class SqlServerSmartSnapshotChangeEventSource extends SqlServerSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSmartSnapshotChangeEventSource.class);

    private final SqlServerConnectorConfig connectorConfig;
    private volatile Lsn smartSnapshotLsn;
    private volatile List<TableId> smartSnapshotTables;
    private volatile List<TableId> uncapturedEligibleTables;

    public SqlServerSmartSnapshotChangeEventSource(SqlServerConnectorConfig connectorConfig,
                                                   MainConnectionProvidingConnectionFactory<SqlServerConnection> connectionFactory,
                                                   SqlServerDatabaseSchema schema, EventDispatcher<SqlServerPartition, TableId> dispatcher, Clock clock,
                                                   SnapshotProgressListener<SqlServerPartition> snapshotProgressListener,
                                                   NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService,
                                                   SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
    }

    /**
     * Set by the coordinator once it has read {@code L_db} + this task's own shard (via
     * {@code SnapshotCoordinationFacade.tablesForTask}) at the matching epoch from the coordination topic.
     * {@code uncapturedEligibleTables} is non-empty only for the schema-history writer (task.id==0) and only
     * under {@code store.only.captured.tables.ddl=false} -- the "eligible for schema but not data-captured"
     * leftover set (design §6.2); every other task gets an empty list.
     */
    public void setSmartSnapshotShard(Lsn lsn, List<TableId> tables, List<TableId> uncapturedEligibleTables) {
        this.smartSnapshotLsn = lsn;
        this.smartSnapshotTables = tables;
        this.uncapturedEligibleTables = uncapturedEligibleTables;
    }

    @Override
    protected void determineCapturedTables(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> ctx,
                                           Set<Pattern> dataCollectionsToBeSnapshotted, SnapshottingTask snapshottingTask)
            throws Exception {
        ctx.capturedTables = new LinkedHashSet<>(smartSnapshotTables);
        LinkedHashSet<TableId> schemaTables = new LinkedHashSet<>(smartSnapshotTables);
        schemaTables.addAll(uncapturedEligibleTables);
        ctx.capturedSchemaTables = schemaTables;
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> ctx,
                                           SqlServerOffsetContext previousOffset)
            throws Exception {
        if (previousOffset != null && !snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot()) {
            ctx.offset = previousOffset;
            tryStartingSnapshot(ctx);
            return;
        }

        // L_db comes from coordination (captured once by the Connector), never from this task's own
        // getMaxLsn() -- every shard must stream from the identical anchor (design §11.0).
        ctx.offset = new SqlServerOffsetContext(connectorConfig, TxLogPosition.valueOf(smartSnapshotLsn), null, false);
        LOGGER.info("Smart snapshot: [{}/{}] set offset LSN={}, shard={}",
                ctx.partition.getDatabaseName(), connectorConfig.getTaskId(), smartSnapshotLsn, smartSnapshotTables);
    }
}
