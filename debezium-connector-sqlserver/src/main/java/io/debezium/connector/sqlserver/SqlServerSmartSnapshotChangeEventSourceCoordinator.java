/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * Snapshot-only coordinator for a smart-snapshot shard task: polls the shared {@code snapshot_info} published
 * by the task-0 leader, computes its own shard via {@link SnapshotCoordinationFacade#tablesForTask}, snapshots
 * it, then writes its completion marker and idles until Connect downscales to streaming.
 *
 * <p>Failure / rejoin / stale-{@code L_db} all signal {@code restart_needed}; the framework monitor then bumps
 * the epoch and reconfigures, so one task failure triggers a full-round restart (mirrors Postgres). A non-0
 * shard blocks until task-0 has written the full schema history (the schema barrier).
 */
public class SqlServerSmartSnapshotChangeEventSourceCoordinator extends SqlServerChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSmartSnapshotChangeEventSourceCoordinator.class);
    private static final int POLL_RETRY_COUNT = 30;
    private static final long POLL_INTERVAL_MS = 10_000;

    private final int epoch;
    private final SnapshotCoordinationFacade snapshotCoordination;
    private final String taskId;
    private final boolean isSchemaHistoryWriter; // task-0
    // task-0 only: the eligible-but-uncaptured leftover set, discovered by this task's leader thread and
    // handed over in-memory (empty for other tasks).
    private final Supplier<List<TableId>> uncapturedEligibleSupplier;
    private final Supplier<SqlServerConnection> connectionSupplier;

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
                                                              int epoch, SnapshotCoordinationFacade snapshotCoordination, String taskId,
                                                              Supplier<List<TableId>> uncapturedEligibleSupplier,
                                                              Supplier<SqlServerConnection> connectionSupplier) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
                changeEventSourceMetricsFactory, eventDispatcher, schema, clock, signalProcessor, notificationService, snapshotterService);
        this.epoch = epoch;
        this.snapshotCoordination = snapshotCoordination;
        this.taskId = taskId;
        this.isSchemaHistoryWriter = "0".equals(taskId);
        this.uncapturedEligibleSupplier = uncapturedEligibleSupplier;
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    protected void executeChangeEventSources(CdcSourceTaskContext taskContext,
                                             SnapshotChangeEventSource<SqlServerPartition, SqlServerOffsetContext> snapshotSource,
                                             Offsets<SqlServerPartition, SqlServerOffsetContext> previousOffsets,
                                             AtomicReference<LoggingContext.PreviousContext> previousLogContext,
                                             ChangeEventSourceContext context)
            throws InterruptedException {

        snapshotCoordination.start();

        SqlServerPartition partition = previousOffsets.getTheOnlyPartition();
        previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));
        SqlServerOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        // Epoch mismatch with the previous offset -> stale from an earlier round, clear it.
        if (previousOffset != null) {
            Integer offsetEpoch = previousOffset.getEpoch();
            if (offsetEpoch != null && !offsetEpoch.equals(epoch)) {
                LOGGER.info("Smart snapshot: [{}/{}] epoch mismatch (offset={}, config={}), clearing offset",
                        partition.getDatabaseName(), taskId, offsetEpoch, epoch);
                previousOffsets.resetOffset(partition);
            }
        }

        // Task bounced AFTER finishing -> idle and let the monitor downscale (not a rejoin).
        if (snapshotCoordination.isTaskDone(taskId, epoch)) {
            LOGGER.info("Smart snapshot: [{}/{}] already completed @epoch {}, idling", partition.getDatabaseName(), taskId, epoch);
            idleUntilRestart(context);
            return;
        }

        // Rejoin: a join marker for this (task, epoch) means this task already attached and is now restarting
        // mid-round -- it may hold partial data, so signal restart_needed (one task failure -> full restart).
        Integer joinEpoch = snapshotCoordination.readTaskJoinEpoch(taskId);
        if (joinEpoch != null && joinEpoch == epoch) {
            LOGGER.warn("Smart snapshot: [{}/{}] rejoin detected @epoch {}, signaling restart_needed", partition.getDatabaseName(), taskId, epoch);
            writeRestartNeeded();
            idleUntilRestart(context);
            return;
        }

        // Stale-epoch: the connector already moved to a newer round than this task's config knows.
        Integer savedEpoch = snapshotCoordination.readEpoch();
        if (savedEpoch != null && savedEpoch > epoch) {
            LOGGER.info("Smart snapshot: [{}/{}] connector epoch {} ahead of this task's epoch {}, idling",
                    partition.getDatabaseName(), taskId, savedEpoch, epoch);
            idleUntilRestart(context);
            return;
        }

        // Fresh join -- write the marker before attaching, so any later restart is caught as a rejoin above.
        snapshotCoordination.writeTaskJoin(taskId, epoch);

        ShardAssignment shard = awaitSnapshotInfo(partition);

        if (isLDbStale(partition, shard)) {
            LOGGER.warn("Smart snapshot: [{}/{}] L_db={} (epoch {}) aged past CDC retention, signaling restart_needed",
                    partition.getDatabaseName(), taskId, shard.lsn, epoch);
            writeRestartNeeded();
            idleUntilRestart(context);
            return;
        }

        // Schema barrier: a non-0 shard must not snapshot data until task-0 has written the full schema
        // history. task-0 produces this signal (in its source) and does not wait.
        if (!isSchemaHistoryWriter) {
            awaitSchemaWritten(partition, context);
        }

        List<TableId> schemaHistoryTables = List.of();
        if (isSchemaHistoryWriter) {
            // task-0 owns the full schema history: all captured tables + the eligible-but-uncaptured leftover.
            schemaHistoryTables = new ArrayList<>(shard.allTables);
            schemaHistoryTables.addAll(uncapturedEligibleSupplier.get());
        }
        ((SqlServerSmartSnapshotChangeEventSource) snapshotSource).setSmartSnapshotShard(
                shard.lsn, shard.tables, schemaHistoryTables, snapshotCoordination, epoch);

        SnapshotResult<SqlServerOffsetContext> snapshotResult;
        try {
            snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);
            LOGGER.info("Smart snapshot: [{}/{}] snapshot completed status={}", partition.getDatabaseName(), taskId, snapshotResult.getStatus());
        }
        catch (InterruptedException e) {
            throw e; // shutdown, not a snapshot failure -- do not write completion
        }
        catch (Exception e) {
            if (Thread.currentThread().isInterrupted()) {
                LOGGER.warn("Smart snapshot: [{}/{}] interrupted during snapshot, exiting", partition.getDatabaseName(), taskId, e);
                return;
            }
            LOGGER.warn("Smart snapshot: [{}/{}] epoch-{} snapshot failed, signaling restart_needed", partition.getDatabaseName(), taskId, epoch, e);
            writeRestartNeeded();
            throw new DebeziumException(
                    String.format("Smart snapshot: [%s/%s] epoch-%d snapshot failed", partition.getDatabaseName(), taskId, epoch), e);
        }

        if (!snapshotResult.isCompletedOrSkipped()) {
            throw new DebeziumException(String.format(
                    "Smart snapshot: [%s/%s] epoch-%d snapshot ended with unexpected status %s; not recording completion",
                    partition.getDatabaseName(), taskId, epoch, snapshotResult.getStatus()));
        }

        writeCompleted();
        idleUntilRestart(context);
    }

    private static class ShardAssignment {
        final Lsn lsn;
        final List<TableId> tables; // this task's shard (data)
        final List<TableId> allTables; // full captured set (for task-0's schema history)

        ShardAssignment(Lsn lsn, List<TableId> tables, List<TableId> allTables) {
            this.lsn = lsn;
            this.tables = tables;
            this.allTables = allTables;
        }
    }

    /**
     * Polls the shared snapshot-info record for this task's epoch, then deterministically computes its shard.
     * The leader publishes before any task snapshots, so this normally resolves on the first read; the retry
     * loop only covers topic replication lag.
     */
    private ShardAssignment awaitSnapshotInfo(SqlServerPartition partition) throws InterruptedException {
        for (int attempt = 0; attempt < POLL_RETRY_COUNT; attempt++) {
            Map<String, Object> snapshotInfo = snapshotCoordination.readSnapshotInfo();
            if (snapshotInfo != null && snapshotInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT) != null) {
                Integer infoEpoch = SnapshotCoordinationFacade.epochOf(snapshotInfo);
                if (infoEpoch != null && infoEpoch == epoch) {
                    Lsn lsn = Lsn.valueOf(String.valueOf(snapshotInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT)));
                    List<TableId> allTables = SnapshotCoordinationFacade.parseTablesPostgres(snapshotInfo.get(SnapshotCoordinationFacade.TABLES));
                    int numTasks = ((Number) snapshotInfo.get(SnapshotCoordinationFacade.NUM_TASKS)).intValue();
                    List<TableId> myShard = SnapshotCoordinationFacade.tablesForTask(allTables, Integer.parseInt(taskId), numTasks);
                    LOGGER.info("Smart snapshot: [{}/{}] got L_db={}, shard={}, epoch={}",
                            partition.getDatabaseName(), taskId, lsn, myShard, epoch);
                    return new ShardAssignment(lsn, myShard, allTables);
                }
            }
            LOGGER.info("Smart snapshot: [{}/{}] waiting for snapshot-info (attempt {}/{})",
                    partition.getDatabaseName(), taskId, attempt + 1, POLL_RETRY_COUNT);
            Thread.sleep(POLL_INTERVAL_MS);
        }
        throw new DebeziumException(
                String.format("Smart snapshot: [%s/%s] timed out waiting for snapshot-info", partition.getDatabaseName(), taskId));
    }

    /** Blocks until task-0 has written the full schema history for this epoch (the schema barrier). */
    private void awaitSchemaWritten(SqlServerPartition partition, ChangeEventSourceContext context) throws InterruptedException {
        for (int attempt = 0; attempt < POLL_RETRY_COUNT && context.isRunning(); attempt++) {
            if (snapshotCoordination.isTaskStartedTransaction("0", epoch)) {
                return;
            }
            LOGGER.info("Smart snapshot: [{}/{}] waiting for task-0 schema history @epoch {} (attempt {}/{})",
                    partition.getDatabaseName(), taskId, epoch, attempt + 1, POLL_RETRY_COUNT);
            Thread.sleep(POLL_INTERVAL_MS);
        }
        if (!snapshotCoordination.isTaskStartedTransaction("0", epoch)) {
            throw new DebeziumException(
                    String.format("Smart snapshot: [%s/%s] timed out waiting for task-0 schema history @epoch %d",
                            partition.getDatabaseName(), taskId, epoch));
        }
    }

    /**
     * True if {@code L_db} has aged past CDC change-table retention for any of this shard's tables -- in which
     * case re-snapshotting is futile because the streaming handoff from {@code L_db} would fail.
     */
    private boolean isLDbStale(SqlServerPartition partition, ShardAssignment shard) {
        try (SqlServerConnection connection = connectionSupplier.get()) {
            for (SqlServerChangeTable changeTable : connection.getChangeTables(partition.getDatabaseName())) {
                if (!shard.tables.contains(changeTable.getSourceTableId())) {
                    continue;
                }
                Lsn minLsn = connection.getMinLsn(partition.getDatabaseName(), changeTable.getCaptureInstance());
                if (minLsn.isAvailable() && minLsn.compareTo(shard.lsn) > 0) {
                    LOGGER.warn("Smart snapshot: [{}/{}] L_db={} aged past CDC retention for {} (min_lsn={})",
                            partition.getDatabaseName(), taskId, shard.lsn, changeTable.getSourceTableId(), minLsn);
                    return true;
                }
            }
            return false;
        }
        catch (SQLException e) {
            throw new DebeziumException(
                    String.format("Smart snapshot: [%s/%s] failed to check CDC retention for L_db=%s", partition.getDatabaseName(), taskId, shard.lsn), e);
        }
    }

    private void idleUntilRestart(ChangeEventSourceContext context) throws InterruptedException {
        int counter = 0;
        while (context.isRunning()) {
            Thread.sleep(10_000);
            if (counter++ % 3 == 0) {
                LOGGER.info("Smart snapshot: [{}] shard task idling", taskId);
            }
        }
    }

    private void writeCompleted() {
        try {
            snapshotCoordination.writeTaskDone(taskId, epoch);
        }
        catch (Exception e) {
            throw new DebeziumException(
                    String.format("Smart snapshot: [%s] failed to write completion @epoch %d", taskId, epoch), e);
        }
    }

    private void writeRestartNeeded() {
        try {
            snapshotCoordination.writeRestartNeeded(taskId, epoch);
        }
        catch (Exception e) {
            // Can't signal; fail the task. The join marker still triggers the rejoin path on the next start.
            throw new DebeziumException(
                    String.format("Smart snapshot: [%s] failed to write restart_needed @epoch %d", taskId, epoch), e);
        }
    }
}
