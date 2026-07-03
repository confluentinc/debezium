/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Collect;
import io.debezium.util.LoggingContext;

/**
 * Snapshot-only coordinator for a smart-snapshot shard task (design §4.4/§7): reads {@code L_db} + epoch from
 * the per-database coordination topic (published by the Connector -- design §3.4/§11.0), runs the shard's
 * snapshot, writes its completion record, then idles until Connect swaps it into the collapsed streaming
 * layout. Never transitions to streaming itself.
 *
 * <p>Unlike {@code PostgresSmartSnapshotChangeEventSourceCoordinator}, there is no join-marker/rejoin-detection
 * or {@code restart_needed} signaling: under {@code repeatable_read} a restarted shard task has nothing
 * unrejoinable to protect (no exported snapshot, no held connection) -- it simply re-reads its shard from
 * scratch at the same epoch/L_db (design §7.1).
 */
public class SqlServerSmartSnapshotChangeEventSourceCoordinator extends SqlServerChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSmartSnapshotChangeEventSourceCoordinator.class);
    private static final int MAX_L_DB_POLL_ATTEMPTS = 30;
    private static final long L_DB_POLL_INTERVAL_MS = 10_000;

    private final int epoch;
    private final SnapshotCoordination snapshotCoordination;
    private final String serverName;
    private final String databaseTaskIndex;

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
                                                              int epoch, SnapshotCoordination snapshotCoordination,
                                                              String serverName, String databaseTaskIndex) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
                changeEventSourceMetricsFactory, eventDispatcher, schema, clock, signalProcessor, notificationService, snapshotterService);
        this.epoch = epoch;
        this.snapshotCoordination = snapshotCoordination;
        this.serverName = serverName;
        this.databaseTaskIndex = databaseTaskIndex;
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
                        partition.getDatabaseName(), databaseTaskIndex, offsetEpoch, epoch);
                previousOffsets.resetOffset(partition);
                previousOffset = null;
            }
        }

        // A non-null previousOffset here already matches this epoch (the mismatch block above cleared it
        // otherwise). If it shows the snapshot already completed, this is the task being bounced AFTER
        // finishing -- idle and let the Connector's monitor downscale.
        Map<String, Object> done = snapshotCoordination.read(
                SmartSnapshotConnectorCoordinator.completedKey(serverName, databaseTaskIndex));
        Integer doneEpoch = SmartSnapshotConnectorCoordinator.readEpoch(done);
        if (done != null
                && Boolean.TRUE.equals(done.get(SmartSnapshotConnectorCoordinator.COMPLETED_KEY))
                && doneEpoch != null && doneEpoch == epoch) {
            LOGGER.info("Smart snapshot: [{}/{}] already completed @epoch {}, idling", partition.getDatabaseName(), databaseTaskIndex, epoch);
            idleUntilRestart(context);
            return;
        }

        Lsn lsn = awaitAnchor(partition);

        ((SqlServerSmartSnapshotChangeEventSource) snapshotSource).setSmartSnapshotLsn(lsn);

        try {
            SnapshotResult<SqlServerOffsetContext> snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);
            LOGGER.info("Smart snapshot: [{}/{}] snapshot completed status={}", partition.getDatabaseName(), databaseTaskIndex, snapshotResult.getStatus());
        }
        catch (InterruptedException e) {
            throw e; // shutdown, not a snapshot failure -- do not write completion
        }
        catch (Exception e) {
            // No restart_needed signal here (unlike Postgres): a plain restart re-reads the same epoch/L_db
            // and re-snapshots the shard from scratch, which is always safe under repeatable_read (§7.1).
            throw new DebeziumException(
                    String.format("Smart snapshot: [%s/%s] epoch-%d snapshot failed", partition.getDatabaseName(), databaseTaskIndex, epoch), e);
        }

        writeCompleted();
        idleUntilRestart(context);
    }

    /**
     * Poll the shared per-database coordination record for {@code slot_lsn} at the matching epoch. The
     * Connector publishes this before any task is ever started (design §5 step 2-3), so under normal
     * operation this resolves on the first read; the retry loop only matters if the task somehow starts
     * before that write is visible (e.g. topic replication lag).
     */
    private Lsn awaitAnchor(SqlServerPartition partition) throws InterruptedException {
        Map<String, String> sharedKey = Collect.hashMapOf("server", serverName);
        for (int attempt = 0; attempt < MAX_L_DB_POLL_ATTEMPTS; attempt++) {
            Map<String, Object> coordData = snapshotCoordination.read(sharedKey);
            if (coordData != null && coordData.get(SmartSnapshotConnectorCoordinator.SLOT_LSN_KEY) != null) {
                Integer coordEpoch = SmartSnapshotConnectorCoordinator.readEpoch(coordData);
                if (coordEpoch != null && coordEpoch == epoch) {
                    String lsnStr = String.valueOf(coordData.get(SmartSnapshotConnectorCoordinator.SLOT_LSN_KEY));
                    LOGGER.info("Smart snapshot: [{}/{}] got L_db={}, executing shard snapshot, epoch={}",
                            partition.getDatabaseName(), databaseTaskIndex, lsnStr, epoch);
                    return Lsn.valueOf(lsnStr);
                }
            }
            LOGGER.info("Smart snapshot: [{}/{}] waiting for L_db (attempt {}/{})",
                    partition.getDatabaseName(), databaseTaskIndex, attempt + 1, MAX_L_DB_POLL_ATTEMPTS);
            Thread.sleep(L_DB_POLL_INTERVAL_MS);
        }
        throw new DebeziumException(
                String.format("Smart snapshot: [%s/%s] timed out waiting for L_db from coordination", partition.getDatabaseName(), databaseTaskIndex));
    }

    private void idleUntilRestart(ChangeEventSourceContext context) throws InterruptedException {
        int counter = 0;
        while (context.isRunning()) {
            Thread.sleep(10_000);
            if (counter % 3 == 0) {
                LOGGER.info("Smart snapshot: [{}] shard task is idling", databaseTaskIndex);
            }
            counter++;
        }
    }

    private void writeCompleted() {
        try {
            Map<String, Object> data = new HashMap<>();
            data.put(SmartSnapshotConnectorCoordinator.COMPLETED_KEY, true);
            data.put(SmartSnapshotConnectorCoordinator.EPOCH_KEY, epoch);
            snapshotCoordination.write(SmartSnapshotConnectorCoordinator.completedKey(serverName, databaseTaskIndex), data);
        }
        catch (Exception e) {
            // can't record completion -> the monitor would never downscale; fail so the task retries
            throw new DebeziumException(
                    String.format("Smart snapshot: [%s] failed to write completed @epoch %d", databaseTaskIndex, epoch), e);
        }
    }
}
