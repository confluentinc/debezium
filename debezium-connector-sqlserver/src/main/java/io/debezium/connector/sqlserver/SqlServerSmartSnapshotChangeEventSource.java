/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Sharded snapshot task change-event source for the smart-snapshot {@code repeatable_read} design: reads
 * {@code L_db} from coordination (injected by {@link SqlServerSmartSnapshotChangeEventSourceCoordinator}) instead
 * of capturing its own (design §11.0 -- only the Connector captures the anchor), and suppresses schema-change
 * dispatch for every task except the designated schema-history writer (design §6.2).
 *
 * <p>Schema *locking* is intentionally left untouched: the inherited {@code lockTablesForSchemaSnapshot}/
 * {@code releaseSchemaSnapshotLocks} already do the per-table {@code TABLOCKX}-then-release dance that this
 * design wants (§6.1) -- there is no write barrier to skip, unlike the {@code snapshot}-isolation design.
 */
public class SqlServerSmartSnapshotChangeEventSource extends SqlServerSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSmartSnapshotChangeEventSource.class);

    private final SqlServerConnectorConfig connectorConfig;
    private Lsn smartSnapshotLsn;

    public SqlServerSmartSnapshotChangeEventSource(SqlServerConnectorConfig connectorConfig,
                                                   MainConnectionProvidingConnectionFactory<SqlServerConnection> connectionFactory,
                                                   SqlServerDatabaseSchema schema, EventDispatcher<SqlServerPartition, TableId> dispatcher, Clock clock,
                                                   SnapshotProgressListener<SqlServerPartition> snapshotProgressListener,
                                                   NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService,
                                                   SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
    }

    /** Set by the coordinator once it has read {@code L_db} + matching epoch from the coordination topic. */
    public void setSmartSnapshotLsn(Lsn lsn) {
        this.smartSnapshotLsn = lsn;
    }

    private boolean isSchemaHistoryWriter() {
        return "0".equals(connectorConfig.getSmartSnapshotDatabaseTaskIndex());
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
        LOGGER.info("Smart snapshot: [{}/{}] set offset LSN={}",
                ctx.partition.getDatabaseName(), connectorConfig.getSmartSnapshotDatabaseTaskIndex(), smartSnapshotLsn);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext,
                                      SqlServerOffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws SQLException, InterruptedException {
        if (!isSchemaHistoryWriter()) {
            super.readTableStructure(sourceContext, snapshotContext, offsetContext, snapshottingTask);
            return;
        }

        // The writer must read the FULL DB structure (design §6.2), not just its own shard, even when
        // store.only.captured.tables.ddl=true would otherwise narrow readTableStructure() to
        // snapshotContext.capturedTables -- else createSchemaChangeEventsForTables() throws for out-of-shard
        // tables. Swap in the full set only for the duration of this call: capturedTables must stay
        // shard-scoped for the subsequent data-read phase.
        Set<TableId> shardTables = snapshotContext.capturedTables;
        snapshotContext.capturedTables = snapshotContext.capturedSchemaTables;
        try {
            super.readTableStructure(sourceContext, snapshotContext, offsetContext, snapshottingTask);
        }
        finally {
            snapshotContext.capturedTables = shardTables;
        }
    }

    @Override
    protected Collection<TableId> getTablesForSchemaChange(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext) {
        if (!isSchemaHistoryWriter()) {
            // non-writers still build their in-memory schema via readTableStructure() above; they just
            // don't dispatch it -- only the writer's CREATE TABLE reaches the schema-history topic (§6.2)
            return Collections.emptyList();
        }
        return super.getTablesForSchemaChange(snapshotContext);
    }
}
