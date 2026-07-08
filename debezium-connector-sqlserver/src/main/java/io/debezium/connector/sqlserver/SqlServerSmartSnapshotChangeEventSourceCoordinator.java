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
 * Snapshot-only coordinator for a smart-snapshot shard task: polls the shared snapshot-info record published
 * once by the Connector, computes this task's own table shard via {@link SnapshotCoordinationFacade#tablesForTask},
 * runs the shard's snapshot, writes its completion record, then idles until Connect swaps it into the collapsed
 * streaming layout. Never transitions to streaming itself.
 *
 * <p>An ordinary snapshot failure just fails the task, which re-reads the same epoch/L_db and re-snapshots from
 * scratch on restart -- always safe under {@code repeatable_read} (nothing held between attempts). The one
 * special case is {@code L_db} aging past CDC change-table retention ({@link #isLDbStale}): re-snapshotting can
 * never succeed then, so the task fails with a non-retriable exception. Recovery is a manual connector restart
 * (which captures a fresh {@code L_db} and bumps the epoch). Non-retriable is required: a retriable failure
 * restarts only the task, which re-reads the same stale {@code L_db} and spins -- only a connector restart
 * bumps the epoch.
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
            // Non-retriable by construction (no SQLException/IOException cause): a retriable failure would
            // restart only the task and re-hit the same stale L_db forever; only a connector restart recovers.
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
            // A plain restart re-reads the same epoch/L_db and re-snapshots -- safe under repeatable_read.
            throw new DebeziumException(
                    String.format("Smart snapshot: [%s/%s] epoch-%d snapshot failed", partition.getDatabaseName(), taskId, epoch), e);
        }

        // Record done only for a COMPLETED/SKIPPED result (matches the base coordinator's isCompletedOrSkipped()
        // gate); anything else would let the monitor downscale a shard that captured no data, so fail loudly.
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
     * Polls the shared snapshot-info record for this task's epoch, then deterministically computes its own
     * shard via {@link SnapshotCoordinationFacade#tablesForTask}. The Connector publishes before any task
     * starts, so this normally resolves on the first read; the retry loop only covers topic replication lag.
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
     * True if {@code L_db} has aged past CDC change-table retention for any of this shard's tables (their
     * capture instance's min LSN is beyond {@code L_db}) -- in which case re-snapshotting is futile because
     * the streaming handoff from {@code L_db} would fail. Cheap (one query per table, nothing held).
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
