/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.re2j.Pattern;

import io.debezium.DebeziumException;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Sharded snapshot task change-event source. Reads {@code L_db} and its own table shard from coordination
 * (injected by {@link SqlServerSmartSnapshotChangeEventSourceCoordinator}) instead of discovering tables or
 * capturing its own anchor.
 *
 * <p>Schema history is owned entirely by task-0: task-0 introspects the database, writes CREATE TABLE for all
 * schema-eligible tables to the shared history topic, and then publishes a "schema written" marker. Other shards
 * write no schema history; they block on that marker and then recover their table structure from the history
 * topic task-0 wrote (never introspecting the database themselves), so every shard uses the identical schema
 * task-0 captured at {@code L_db} -- eliminating any DDL drift between per-task database reads. This is safe
 * because any task failure triggers a full-round restart, so there is no partial-schema risk.
 */
public class SqlServerSmartSnapshotChangeEventSource extends SqlServerSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSmartSnapshotChangeEventSource.class);

    private final SqlServerConnectorConfig connectorConfig;
    private final SqlServerDatabaseSchema databaseSchema;
    private final boolean isSchemaHistoryWriter; // task-0
    private volatile Lsn smartSnapshotLsn;
    private volatile List<TableId> smartSnapshotTables; // this task's data shard
    private volatile List<TableId> schemaHistoryTables; // task-0 only: all schema-eligible tables; empty otherwise
    private volatile SnapshotCoordinationFacade snapshotCoordination;
    private volatile int epoch;

    public SqlServerSmartSnapshotChangeEventSource(SqlServerConnectorConfig connectorConfig,
                                                   MainConnectionProvidingConnectionFactory<SqlServerConnection> connectionFactory,
                                                   SqlServerDatabaseSchema schema, EventDispatcher<SqlServerPartition, TableId> dispatcher, Clock clock,
                                                   SnapshotProgressListener<SqlServerPartition> snapshotProgressListener,
                                                   NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService,
                                                   SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.databaseSchema = schema;
        this.isSchemaHistoryWriter = "0".equals(connectorConfig.getTaskId());
    }

    /**
     * Injected by the coordinator once it has this task's {@code L_db} + data shard. {@code schemaHistoryTables}
     * is the full schema-eligible set for task-0 (the schema-history writer) and empty for every other task.
     */
    public void setSmartSnapshotShard(Lsn lsn, List<TableId> tables, List<TableId> schemaHistoryTables,
                                      SnapshotCoordinationFacade snapshotCoordination, int epoch) {
        this.smartSnapshotLsn = lsn;
        this.smartSnapshotTables = tables;
        this.schemaHistoryTables = schemaHistoryTables;
        this.snapshotCoordination = snapshotCoordination;
        this.epoch = epoch;
    }

    @Override
    protected void determineCapturedTables(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> ctx,
                                           Set<Pattern> dataCollectionsToBeSnapshotted, SnapshottingTask snapshottingTask)
            throws Exception {
        // Data: always this task's shard. Schema (drives readTableStructure): task-0 reads structure for ALL
        // schema-eligible tables (it writes them all to history); other tasks read only their shard's structure.
        ctx.capturedTables = new LinkedHashSet<>(smartSnapshotTables);
        ctx.capturedSchemaTables = isSchemaHistoryWriter ? new LinkedHashSet<>(schemaHistoryTables)
                : new LinkedHashSet<>(smartSnapshotTables);
    }

    @Override
    protected Collection<TableId> getTablesForSchemaChange(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> ctx) {
        // Only task-0 writes CREATE TABLE to the shared schema-history topic (the full eligible set); other
        // shards write nothing (avoids duplicate/concurrent writes to one history topic).
        return isSchemaHistoryWriter ? ctx.capturedSchemaTables : Collections.emptyList();
    }

    /**
     * task-0 introspects the database (it is the schema authority and sole history writer). Every other shard
     * skips database structure introspection entirely and instead recovers its table structure from the schema
     * history topic that task-0 already wrote (the coordinator's barrier guarantees task-0 has flushed it before
     * this runs). This makes all shards use the identical schema task-0 captured at {@code L_db}. A shard still
     * needs the CDC change-table metadata for column filtering during the data read, which is one cheap
     * metadata call (not a structure introspection).
     */
    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> ctx,
                                      SqlServerOffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws SQLException, InterruptedException {
        if (isSchemaHistoryWriter) {
            super.readTableStructure(sourceContext, ctx, offsetContext, snapshottingTask);
            return;
        }
        // Recover the full schema (all tables task-0 wrote) from the history topic into the in-memory schema;
        // this also builds each table's record value schema. ctx.offset (= L_db) is already set by
        // determineSnapshotOffset, which runs before readTableStructure.
        // Schema-history recovery is source-scoped (AbstractSchemaHistory only applies records whose source
        // matches a recovery-offset source key). task-0 writes the history under a task-agnostic source
        // ({server,database}, see getCreateTableEvent), so recover under that same task-agnostic source -- not
        // this shard's own task-scoped source, which would match nothing.
        SqlServerPartition historyPartition = new SqlServerPartition(
                connectorConfig.getLogicalName(), ctx.partition.getDatabaseName(), false, null);
        databaseSchema.recover(Offsets.of(historyPartition, ctx.offset));
        // Copy this shard's recovered tables into the snapshot context, which is what createDataEvents reads to
        // build the SELECT column list.
        for (TableId tableId : smartSnapshotTables) {
            Table table = databaseSchema.tableFor(tableId);
            if (table == null) {
                throw new DebeziumException("Smart snapshot: [" + ctx.partition.getDatabaseName() + "/"
                        + connectorConfig.getTaskId() + "] shard table " + tableId + " not found in schema history "
                        + "recovered from task-0 -- the history topic may be incomplete for this epoch");
            }
            ctx.tables.overwriteTable(table);
        }
        // Populate the CDC change-table map used by the column filter during the data read.
        registerChangeTables(ctx);
        LOGGER.info("Smart snapshot: [{}/{}] recovered {} shard table structure(s) from task-0's schema history",
                ctx.partition.getDatabaseName(), connectorConfig.getTaskId(), smartSnapshotTables.size());
    }

    @Override
    protected void createSchemaChangeEventsForTables(ChangeEventSourceContext sourceContext,
                                                     RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> ctx,
                                                     SnapshottingTask snapshottingTask)
            throws Exception {
        if (isSchemaHistoryWriter) {
            // task-0: emit CREATE TABLE for the full eligible set to the shared schema-history topic (see
            // getTablesForSchemaChange), then signal schema-ready so the other shards can proceed.
            super.createSchemaChangeEventsForTables(sourceContext, ctx, snapshottingTask);
            snapshotCoordination.writeTaskStartedTransaction("0", epoch);
            LOGGER.info("Smart snapshot: [{}/0] schema history written for {} table(s), signaled schema-ready @epoch {}",
                    ctx.partition.getDatabaseName(), ctx.capturedSchemaTables.size(), epoch);
            return;
        }
        // Non-leader shards write NOTHING to schema history (task-0 is the sole writer). Their table structure
        // was already recovered into the in-memory schema in readTableStructure, so there is nothing to emit
        // here -- just perform the base method's snapshot-start side effect.
        tryStartingSnapshot(ctx);
    }

    /**
     * Only task-0 emits schema history (it is the sole writer). Stamp the history records with a task-agnostic
     * source ({@code {server, database}}, no task id) rather than task-0's own task-scoped partition, so every
     * consumer -- sibling shards recovering their structure, and the post-downscale streaming task -- can recover
     * it with the standard partition source (schema-history recovery is source-scoped).
     */
    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> ctx,
                                                    Table table)
            throws SQLException {
        SqlServerPartition taskAgnostic = new SqlServerPartition(
                connectorConfig.getLogicalName(), ctx.partition.getDatabaseName(), false, null);
        return SchemaChangeEvent.ofSnapshotCreate(taskAgnostic, ctx.offset, ctx.catalogName, table);
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

        // L_db comes from coordination (captured once by task-0's leader), never this task's own getMaxLsn() --
        // every shard must stream from the identical anchor.
        ctx.offset = new SqlServerOffsetContext(connectorConfig, TxLogPosition.valueOf(smartSnapshotLsn), null, false);
        LOGGER.info("Smart snapshot: [{}/{}] set offset LSN={}, shard={}",
                ctx.partition.getDatabaseName(), connectorConfig.getTaskId(), smartSnapshotLsn, smartSnapshotTables);
    }
}
