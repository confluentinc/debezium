/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.BinlogOffsetContext;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.SmartSnapshotHeldConnectionRegistry;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Leader-side snapshot setup for MySQL smart snapshot (minimal locking mode only in v1).
 *
 * <p>MySQL has no exportable cross-connection snapshot, so the only way to put N tasks on one consistent point
 * is to block writes while every task opens its own {@code START TRANSACTION WITH CONSISTENT SNAPSHOT}. This
 * manager, running on the leader (task-0) prep thread, acquires the write-blocking lock, captures the binlog
 * position {@code P}, enumerates the captured tables, and writes the COMPLETE, ordered schema history for all
 * tables stamped at {@code P} (the single writer). The lock is held until {@link #onAllTasksStartedTransaction()}
 * (all tasks attached), then released.
 *
 * <p>Locking, position capture, schema read and history write all reuse the single-task machinery via
 * {@link MySqlLeaderSchemaSource}, which runs the normal snapshot sub-sequence
 * (lock → offset → read schema → write history) but deliberately stops before releasing the lock and before
 * copying any data — the followers copy the data slices, and this manager releases the lock.
 */
public class MySqlSmartSnapshotLifecycleManager implements SmartSnapshotLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlSmartSnapshotLifecycleManager.class);

    private final MySqlConnectorConfig connectorConfig;
    private final MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory;
    private final MySqlDatabaseSchema schema;
    private final EventDispatcher<MySqlPartition, TableId> dispatcher;
    private final Clock clock;
    private final MySqlSnapshotChangeEventSourceMetrics metrics;
    private final NotificationService<MySqlPartition, MySqlOffsetContext> notificationService;
    private final SnapshotterService snapshotterService;
    private final int epoch;

    // Holds the leader's lock connection open until all tasks have attached; shared thread-safe close/keepAlive.
    private final SmartSnapshotHeldConnectionRegistry heldConnections;

    public MySqlSmartSnapshotLifecycleManager(MySqlConnectorConfig connectorConfig,
                                              MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
                                              MySqlDatabaseSchema schema,
                                              EventDispatcher<MySqlPartition, TableId> dispatcher,
                                              Clock clock,
                                              MySqlSnapshotChangeEventSourceMetrics metrics,
                                              NotificationService<MySqlPartition, MySqlOffsetContext> notificationService,
                                              SnapshotterService snapshotterService,
                                              int epoch) {
        this.connectorConfig = connectorConfig;
        this.connectionFactory = connectionFactory;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.metrics = metrics;
        this.notificationService = notificationService;
        this.snapshotterService = snapshotterService;
        this.epoch = epoch;
        this.heldConnections = new SmartSnapshotHeldConnectionRegistry("Smart snapshot: [role=leader epoch=" + epoch + "]");
    }

    @Override
    public SnapshotSetup prepareSnapshot(boolean shouldStream) {
        try {
            final BinlogConnectorConnection holder = connectionFactory.newConnection();
            heldConnections.registerConnection("lock", holder);
            // executeWithoutCommitting holds the lock / consistent-snapshot transaction across statements.
            holder.connection().setAutoCommit(false);

            MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> heldFactory = new MainConnectionProvidingConnectionFactory<>() {
                @Override
                public BinlogConnectorConnection mainConnection() {
                    return holder;
                }

                @Override
                public BinlogConnectorConnection newConnection() {
                    return connectionFactory.newConnection();
                }
            };
            MySqlLeaderSchemaSource leaderSource = new MySqlLeaderSchemaSource(
                    connectorConfig, heldFactory, schema, dispatcher, clock, metrics, notificationService, snapshotterService);

            // The full schema history must be recorded under the SHARED {server} source partition (no task id).
            // The post-downscale streaming task runs with taskId=null → partition {server}, and schema-history
            // recovery only applies records whose source partition matches the recovering task's. A task-scoped
            // partition ({server,task:0}) here would make recovery skip every record and start with an empty
            // schema. databaseName is unused for MySQL table enumeration (all readable DBs are scanned).
            MySqlPartition leaderPartition = new MySqlPartition(connectorConfig.getLogicalName(), "");

            // Reuses the single-task lock + position capture + full schema-history write, keeping the lock held.
            MySqlLeaderSchemaSource.Result result = leaderSource.lockCaptureAndWriteHistory(leaderPartition, RUNNING_CONTEXT);

            String consistentPosition = result.binlogFile + ":" + result.binlogPosition + ":"
                    + (result.gtidSet == null ? "" : result.gtidSet);
            LOGGER.info("Smart snapshot: [role=leader epoch={}] Locked, captured P=({}), wrote schema history for {} tables",
                    epoch, consistentPosition, result.tables.size());
            return new SnapshotSetup(null, consistentPosition, result.tables);
        }
        catch (Exception e) {
            releaseSnapshot();
            throw new DebeziumException("Smart snapshot: [role=leader epoch=" + epoch + "] Failed to prepare snapshot", e);
        }
    }

    @Override
    public void onAllTasksStartedTransaction() {
        LOGGER.info("Smart snapshot: [role=leader epoch={}] All tasks attached; releasing the write lock", epoch);
        releaseSnapshot();
    }

    @Override
    public void keepAlive() {
        heldConnections.keepAlive();
    }

    @Override
    public void releaseSnapshot() {
        // Closing the lock connection releases the read lock. If a prepareSnapshot is still mid-query (e.g. the
        // long history write), the close aborts that query so the prep thread can finish.
        heldConnections.close();
    }

    /**
     * Leader-only snapshot source that reuses the single-task snapshot sub-sequence to acquire the lock, capture
     * the binlog position, and write the full schema history — then stops (no lock release, no data copy).
     */
    static class MySqlLeaderSchemaSource extends MySqlSnapshotChangeEventSource {

        MySqlLeaderSchemaSource(MySqlConnectorConfig connectorConfig,
                                MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
                                MySqlDatabaseSchema schema,
                                EventDispatcher<MySqlPartition, TableId> dispatcher,
                                Clock clock,
                                MySqlSnapshotChangeEventSourceMetrics metrics,
                                NotificationService<MySqlPartition, MySqlOffsetContext> notificationService,
                                SnapshotterService snapshotterService) {
            super(connectorConfig, connectionFactory, schema, dispatcher, clock, metrics,
                    record -> {
                    }, () -> {
                    }, notificationService, snapshotterService);
        }

        /**
         * Runs the normal snapshot sub-sequence for the full captured-table set — lock, capture P, read schema,
         * write the complete history (single writer) — but deliberately skips {@code releaseSchemaSnapshotLocks}
         * (the lock stays held until the lifecycle releases it) and {@code createDataEvents} (followers copy the
         * data). Mirrors {@code RelationalSnapshotChangeEventSource.doExecute} steps 3–6.
         */
        Result lockCaptureAndWriteHistory(MySqlPartition partition, ChangeEventSourceContext running) throws Exception {
            SnapshottingTask task = getSnapshottingTask(partition, null);
            @SuppressWarnings("unchecked")
            RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> ctx = (RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext>) prepare(
                    partition, false);

            determineCapturedTables(ctx, getDataCollectionPattern(task.getDataCollections()), task);
            // tier-1 global FLUSH TABLES WITH READ LOCK; the table-list (RDS) fallback runs inside
            // readTableStructure's two-phase path if the global lock was unavailable.
            lockTablesForSchemaSnapshot(running, ctx);
            determineSnapshotOffset(ctx, null);
            readTableStructure(running, ctx, null, task);
            createSchemaChangeEventsForTables(running, ctx, task);

            final Map<String, ?> offset = ctx.offset.getOffset();
            final String binlogFile = (String) offset.get(SourceInfo.BINLOG_FILENAME_OFFSET_KEY);
            final long binlogPosition = ((Number) offset.get(SourceInfo.BINLOG_POSITION_OFFSET_KEY)).longValue();
            final Object gtids = offset.get(BinlogOffsetContext.GTID_SET_KEY);
            return new Result(new ArrayList<>(ctx.capturedTables), binlogFile, binlogPosition, gtids == null ? null : gtids.toString());
        }

        static class Result {
            final List<TableId> tables;
            final String binlogFile;
            final long binlogPosition;
            final String gtidSet;

            Result(List<TableId> tables, String binlogFile, long binlogPosition, String gtidSet) {
                this.tables = tables;
                this.binlogFile = binlogFile;
                this.binlogPosition = binlogPosition;
                this.gtidSet = gtidSet;
            }
        }
    }
}
