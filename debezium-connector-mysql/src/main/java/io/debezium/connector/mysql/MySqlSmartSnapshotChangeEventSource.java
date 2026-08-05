/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.re2j.Pattern;

import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.function.BlockingConsumer;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * MySQL smart snapshot source used by every data task (task-0 foreground included). Unlike Postgres, MySQL has
 * no exportable snapshot to attach to; instead the follower opens its own {@code START TRANSACTION WITH
 * CONSISTENT SNAPSHOT} while the leader holds the global write lock, so all tasks freeze at the same binlog
 * position {@code P}. Schema is applied in memory only — the leader task is the sole writer of the schema
 * history — and the offset is stamped with {@code P} from the coordination topic (not this connection's own
 * {@code SHOW MASTER STATUS}).
 */
public class MySqlSmartSnapshotChangeEventSource extends MySqlSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlSmartSnapshotChangeEventSource.class);

    private final MySqlConnectorConfig connectorConfig;
    private final BinlogConnectorConnection connection;
    private final MySqlDatabaseSchema mysqlSchema;
    private final String taskId;

    private volatile SnapshotCoordinationFacade snapshotCoordination;
    private volatile int epoch;
    private volatile String pBinlogFile;
    private volatile long pBinlogPos;
    private volatile String pGtidSet;
    private volatile List<TableId> smartSnapshotTables;

    public MySqlSmartSnapshotChangeEventSource(MySqlConnectorConfig connectorConfig,
                                               MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
                                               MySqlDatabaseSchema schema,
                                               EventDispatcher<MySqlPartition, TableId> dispatcher,
                                               Clock clock,
                                               MySqlSnapshotChangeEventSourceMetrics metrics,
                                               BlockingConsumer<Function<SourceRecord, SourceRecord>> lastEventProcessor,
                                               Runnable preSnapshotAction,
                                               NotificationService<MySqlPartition, MySqlOffsetContext> notificationService,
                                               SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, metrics,
                lastEventProcessor, preSnapshotAction, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.connection = connectionFactory.mainConnection();
        this.mysqlSchema = schema;
        this.taskId = connectorConfig.getTaskId();
    }

    public void setSmartSnapshot(int epoch, String binlogFile, long binlogPos, String gtidSet,
                                 List<TableId> tables, SnapshotCoordinationFacade coordination) {
        this.epoch = epoch;
        this.pBinlogFile = binlogFile;
        this.pBinlogPos = binlogPos;
        this.pGtidSet = gtidSet;
        this.smartSnapshotTables = tables;
        this.snapshotCoordination = coordination;
    }

    @Override
    protected void determineCapturedTables(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> ctx,
                                           Set<Pattern> ignoredSnapshotPatterns, SnapshottingTask snapshottingTask) {
        // This task's slice is already the final, filtered set from the leader; snapshot exactly it.
        LinkedHashSet<TableId> mine = new LinkedHashSet<>(smartSnapshotTables);
        ctx.capturedTables = mine;
        ctx.capturedSchemaTables = mine;
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Using the {}-table slice from the leader", taskId, epoch, mine.size());
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> ctx,
                                           MySqlOffsetContext previousOffset)
            throws Exception {
        // Anchor the offset to P from the coordination topic, not this connection's own SHOW MASTER STATUS, so
        // every task copies as-of the same binlog position.
        MySqlOffsetContext offset = MySqlOffsetContext.initial(connectorConfig, epoch);
        offset.setBinlogStartPoint(pBinlogFile, pBinlogPos);
        offset.setCompletedGtidSet(pGtidSet);
        ctx.offset = offset;
        tryStartingSnapshot(ctx);
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Set offset to P=({}:{}) gtid={}", taskId, epoch, pBinlogFile, pBinlogPos, pGtidSet);
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext)
            throws SQLException {
        // The leader holds the write-blocking lock. The follower takes NO lock; it only opens its own
        // consistent-snapshot transaction while the leader's lock is held, which freezes its reads at P.
        connection.connection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        connection.executeWithoutCommitting("START TRANSACTION WITH CONSISTENT SNAPSHOT");
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Opened consistent-snapshot transaction (leader holds the lock)", taskId, epoch);
    }

    @Override
    protected boolean twoPhaseSchemaSnapshot() {
        // The follower is not globally locked and must NOT take its own table locks / two-phase snapshot.
        return false;
    }

    @Override
    protected void emitSchemaChangeEvent(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext,
                                         SchemaChangeEvent event, TableId tableId) {
        // Apply schema in memory only — no schema-history persist, no public schema-change topic.
        // Only the leader task persists the schema history (single writer).
        mysqlSchema.applySchemaChangeInMemoryOnly(event);
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext) {
        // Signal transaction_started AFTER the schema read, so the leader knows this task has attached and can
        // release the global lock once every task has attached. Let a write failure fail the task.
        snapshotCoordination.writeTaskStartedTransaction(taskId, epoch);
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Signaled task_started_transaction (schema read done)", taskId, epoch);
    }
}
