/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
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
 * Snapshot-only coordinator for a smart-snapshot shard task (design §4.4/§7): polls the shared snapshot-info
 * record published once by the Connector (design §3.4/§11.0), computes this task's own table shard via
 * {@link SnapshotCoordinationFacade#tablesForTask}, runs the shard's snapshot, writes its completion record,
 * then idles until Connect swaps it into the collapsed streaming layout. Never transitions to streaming
 * itself.
 *
 * <p>Unlike {@code PostgresSmartSnapshotChangeEventSourceCoordinator}, there is no join-marker/rejoin-detection
 * or {@code restart_needed} signaling on an ordinary snapshot failure: under {@code repeatable_read} a
 * restarted shard task has nothing unrejoinable to protect (no exported snapshot, no held connection) -- it
 * simply re-reads its shard from scratch at the same epoch/L_db (design §7.1).
 *
 * <p>Retention staleness (design TL;DR "Task restart" exception, §13 F11 / §19): if {@code L_db} has aged past
 * CDC change-table retention for any table in this shard, blindly re-snapshotting is doomed -- the eventual
 * streaming handoff would still fail. For Phase 0 this fails the task with a non-retriable exception rather
 * than trying to auto-recover online (design §19): recovery is a manual connector restart, which re-runs
 * {@code SqlServerConnector.start()} -> {@code bumpEpochIfIncompleteRoundExists} (fresh {@code L_db} + epoch
 * bump) synchronously. The exception is deliberately non-retriable ({@link DebeziumException} with no
 * {@code SQLException}/{@code IOException} cause -- see {@code SqlServerErrorHandler#communicationExceptions}):
 * a *retriable* failure would restart only the task, which re-reads the same epoch/L_db from its config and
 * re-hits the same staleness, spinning forever -- only a *connector* restart bumps the epoch (Gap 1).
 */
public class SqlServerSmartSnapshotChangeEventSourceCoordinator extends SqlServerChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSmartSnapshotChangeEventSourceCoordinator.class);
    private static final int SNAPSHOT_INFO_POLL_RETRY_COUNT = 30;
    private static final long SNAPSHOT_INFO_POLL_INTERVAL_MS = 10_000;

    private final int epoch;
    private final SnapshotCoordinationFacade snapshotCoordination;
    private final String taskId;
    private final List<TableId> uncapturedEligibleTables;
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
                                                              List<TableId> uncapturedEligibleTables, Supplier<SqlServerConnection> connectionSupplier) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
                changeEventSourceMetricsFactory, eventDispatcher, schema, clock, signalProcessor, notificationService, snapshotterService);
        this.epoch = epoch;
        this.snapshotCoordination = snapshotCoordination;
        this.taskId = taskId;
        this.uncapturedEligibleTables = uncapturedEligibleTables;
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

        // Epoch mismatch with previous offset -> stale from an earlier round, clear it
        if (previousOffset != null) {
            Integer offsetEpoch = previousOffset.getEpoch();
            if (offsetEpoch != null && !offsetEpoch.equals(epoch)) {
                LOGGER.info("Smart snapshot: [{}/{}] epoch mismatch (offset={}, config={}), clearing offset",
                        partition.getDatabaseName(), taskId, offsetEpoch, epoch);
                previousOffsets.resetOffset(partition);
                previousOffset = null;
            }
        }

        // A non-null previousOffset here already matches this epoch (the mismatch block above cleared it
        // otherwise). If it shows the snapshot already completed, this is the task being bounced AFTER
        // finishing -- idle and let the Connector's monitor downscale.
        if (snapshotCoordination.isDone(taskId, epoch)) {
            LOGGER.info("Smart snapshot: [{}/{}] already completed @epoch {}, idling", partition.getDatabaseName(), taskId, epoch);
            idleUntilRestart(context);
            return;
        }

        // Stale-epoch check: the connector already moved on to a newer round than this task's config knows.
        Integer savedEpoch = snapshotCoordination.readEpoch();
        if (savedEpoch != null && savedEpoch > epoch) {
            LOGGER.info("Smart snapshot: [{}/{}] connector epoch {} is ahead of this task's epoch {}, idling",
                    partition.getDatabaseName(), taskId, savedEpoch, epoch);
            idleUntilRestart(context);
            return;
        }

        ShardAssignment shard = awaitShardAssignment(partition);

        if (isLDbStale(partition, shard)) {
            // Phase 0 (design §19): fail the task with a non-retriable exception rather than auto-recovering.
            // Non-retriable is deliberate -- a retriable failure restarts only the task, which re-reads the
            // same epoch/L_db and re-hits this same staleness forever; only a connector restart bumps the
            // epoch (Gap 1) and captures a fresh L_db. The message tells the operator to restart the connector.
            throw new DebeziumException(String.format(
                    "Smart snapshot: [%s/%s] L_db=%s (epoch %d) has aged past CDC change-table retention for this shard; "
                            + "a fresh snapshot anchor is required. Restart the CONNECTOR (not just this task) to capture a new "
                            + "L_db and retry -- restarting only the task would re-use the same stale anchor and fail again.",
                    partition.getDatabaseName(), taskId, shard.lsn, epoch));
        }

        ((SqlServerSmartSnapshotChangeEventSource) snapshotSource).setSmartSnapshotShard(shard.lsn, shard.tables, uncapturedEligibleTables);

        SnapshotResult<SqlServerOffsetContext> snapshotResult;
        try {
            snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);
            LOGGER.info("Smart snapshot: [{}/{}] snapshot completed status={}", partition.getDatabaseName(), taskId, snapshotResult.getStatus());
        }
        catch (InterruptedException e) {
            throw e; // shutdown, not a snapshot failure -- do not write completion
        }
        catch (Exception e) {
            // No restart_needed signal here (unlike Postgres): a plain restart re-reads the same epoch/L_db
            // and re-snapshots the shard from scratch, which is always safe under repeatable_read (§7.1).
            throw new DebeziumException(
                    String.format("Smart snapshot: [%s/%s] epoch-%d snapshot failed", partition.getDatabaseName(), taskId, epoch), e);
        }

        // Only record this shard as done for a genuinely COMPLETED (or SKIPPED -- a legitimate "nothing to do")
        // snapshot, matching the base ChangeEventSourceCoordinator's own isCompletedOrSkipped() gate. A
        // non-throwing ABORTED can't occur on this path today (abort surfaces as a thrown exception, handled
        // above), but guard anyway: writing done for a non-completed result would let the monitor downscale a
        // shard that never captured its data. Fail loudly instead.
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
        final List<TableId> tables;

        ShardAssignment(Lsn lsn, List<TableId> tables) {
            this.lsn = lsn;
            this.tables = tables;
        }
    }

    /**
     * Poll the shared snapshot-info record for {@code consistentPosition}/{@code tables} at the matching
     * epoch, then deterministically compute this task's own shard via
     * {@link SnapshotCoordinationFacade#tablesForTask}. The Connector publishes this before any task is ever
     * started (design §5 step 2-3), so under normal operation this resolves on the first read; the retry loop
     * only matters if the task somehow starts before that write is visible (e.g. topic replication lag).
     */
    private ShardAssignment awaitShardAssignment(SqlServerPartition partition) throws InterruptedException {
        for (int attempt = 0; attempt < SNAPSHOT_INFO_POLL_RETRY_COUNT; attempt++) {
            Map<String, Object> snapshotInfo = snapshotCoordination.readSnapshotInfo();
            if (snapshotInfo != null && snapshotInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT) != null) {
                Integer infoEpoch = SnapshotCoordinationFacade.epochOf(snapshotInfo);
                if (infoEpoch != null && infoEpoch == epoch) {
                    Lsn lsn = Lsn.valueOf(String.valueOf(snapshotInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT)));
                    List<TableId> allTables = SnapshotCoordinationFacade.parseTables(snapshotInfo.get(SnapshotCoordinationFacade.TABLES));
                    int numTasks = ((Number) snapshotInfo.get(SnapshotCoordinationFacade.NUM_TASKS)).intValue();
                    List<TableId> myShard = SnapshotCoordinationFacade.tablesForTask(allTables, Integer.parseInt(taskId), numTasks);
                    LOGGER.info("Smart snapshot: [{}/{}] got L_db={}, shard={}, executing shard snapshot, epoch={}",
                            partition.getDatabaseName(), taskId, lsn, myShard, epoch);
                    return new ShardAssignment(lsn, myShard);
                }
            }
            LOGGER.info("Smart snapshot: [{}/{}] waiting for snapshot-info (attempt {}/{})",
                    partition.getDatabaseName(), taskId, attempt + 1, SNAPSHOT_INFO_POLL_RETRY_COUNT);
            Thread.sleep(SNAPSHOT_INFO_POLL_INTERVAL_MS);
        }
        throw new DebeziumException(
                String.format("Smart snapshot: [%s/%s] timed out waiting for snapshot-info from coordination", partition.getDatabaseName(), taskId));
    }

    /**
     * Retention check (design TL;DR "Task restart" exception, §13 F11 / §19): {@code L_db} has aged past CDC
     * change-table retention if any of this shard's tables' capture instance has a min LSN beyond {@code
     * L_db} -- i.e. the change table has already been purged past the point this round is trying to stream
     * from. Checked once per shard task, on every (re)start, before re-snapshotting -- cheap (one query per
     * table, no held connection) and catches the staleness before wasting a re-snapshot that would still be
     * doomed at the eventual streaming handoff.
     */
    private boolean isLDbStale(SqlServerPartition partition, ShardAssignment shard) {
        try (SqlServerConnection connection = connectionSupplier.get()) {
            for (SqlServerChangeTable changeTable : connection.getChangeTables(partition.getDatabaseName())) {
                if (!shard.tables.contains(changeTable.getSourceTableId())) {
                    continue;
                }
                Lsn minLsn = connection.getMinLsn(partition.getDatabaseName(), changeTable.getCaptureInstance());
                if (minLsn.isAvailable() && minLsn.compareTo(shard.lsn) > 0) {
                    LOGGER.warn("Smart snapshot: [{}/{}] L_db={} has aged past CDC retention for {} (min_lsn={}), failing the task",
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
            if (counter % 3 == 0) {
                LOGGER.info("Smart snapshot: [{}] shard task is idling", taskId);
            }
            counter++;
        }
    }

    private void writeCompleted() {
        try {
            snapshotCoordination.writeDone(taskId, epoch);
        }
        catch (Exception e) {
            // can't record completion -> the monitor would never downscale; fail so the task retries
            throw new DebeziumException(
                    String.format("Smart snapshot: [%s] failed to write completed @epoch %d", taskId, epoch), e);
        }
    }
}
