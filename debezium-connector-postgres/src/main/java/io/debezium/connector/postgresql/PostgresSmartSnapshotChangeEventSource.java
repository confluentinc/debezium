/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.re2j.Pattern;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

public class PostgresSmartSnapshotChangeEventSource extends PostgresSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSmartSnapshotChangeEventSource.class);

    private final String taskId;

    // Set once by setSnapshotCoordination() and then read by the snapshot hooks below. Both happen on the
    // same coordinator thread, in order (configureSmartSource() runs before doSnapshot() in
    // AbstractSmartSnapshotChangeEventSourceCoordinator#executeChangeEventSources), so no volatile is needed.
    private SnapshotCoordinationFacade snapshotCoordination;
    private int epoch;
    private String smartSnapshotName;
    private Lsn smartSnapshotLsn;
    private Long smartSnapshotTxId;
    private List<TableId> smartSnapshotTables;

    public PostgresSmartSnapshotChangeEventSource(
                                                  PostgresConnectorConfig connectorConfig,
                                                  SnapshotterService snapshotterService,
                                                  MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory,
                                                  PostgresSchema schema,
                                                  EventDispatcher<PostgresPartition, TableId> dispatcher,
                                                  Clock clock,
                                                  SnapshotProgressListener<PostgresPartition> snapshotProgressListener,
                                                  SlotCreationResult slotCreatedInfo,
                                                  SlotState startingSlotInfo,
                                                  NotificationService<PostgresPartition, PostgresOffsetContext> notificationService) {
        super(connectorConfig, snapshotterService, connectionFactory, schema,
                dispatcher, clock, snapshotProgressListener,
                slotCreatedInfo, startingSlotInfo, notificationService);
        // connectorConfig and jdbcConnection are inherited (protected) from PostgresSnapshotChangeEventSource.
        this.taskId = connectorConfig.getTaskId();
    }

    public void setSnapshotCoordination(
                                        int epoch,
                                        String snapshotName,
                                        Lsn lsn,
                                        Long txId,
                                        List<TableId> tableIds,
                                        SnapshotCoordinationFacade coordination) {
        this.epoch = epoch;
        this.smartSnapshotName = snapshotName;
        this.smartSnapshotLsn = lsn;
        this.smartSnapshotTxId = txId;
        this.smartSnapshotTables = tableIds;
        this.snapshotCoordination = coordination;
    }

    @Override
    protected void determineCapturedTables(
                                           RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx,
                                           Set<Pattern> ignoredSnapshotPatterns, SnapshottingTask snapshottingTask) {
        // INVARIANT: this task's slice is snapshotted exactly as given, with NO further filtering.
        //
        // The published slice is already the final, filtered, sorted set. The leader produced it by running the
        // standard RelationalSnapshotChangeEventSource#determineCapturedTables (via its discoverAndLock), which
        // applies dataCollectionFilter().isIncluded() AND then addSignalingCollectionAndSort(). That combined set
        // is partitioned across tasks and each partition published as one task's assignment, so every slice is a
        // subset of it. Re-running dataCollectionFilter().isIncluded() here would therefore be redundant AND wrong:
        // it would drop the signaling data collection, which the leader adds deliberately even though the data
        // filter excludes it. The signaling collection lands in exactly one task's slice, so it is snapshotted once.
        LinkedHashSet<TableId> mine = new LinkedHashSet<>(smartSnapshotTables);
        ctx.capturedTables = mine;
        ctx.capturedSchemaTables = mine; // unused on the Postgres path (readTableStructure derives schemas from capturedTables)
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Determining captured tables using the slice from the leader", taskId, epoch);
    }

    @Override
    protected void determineSnapshotOffset(
                                           RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx,
                                           PostgresOffsetContext previousOffset)
            throws Exception {
        // Mirror the parent: reuse a pre-set offset if one exists (the on-demand/blocking path sets ctx.offset
        // before this runs), otherwise build one. initialContext() wires up sourceInfo, the transaction/
        // incremental-snapshot contexts, and stamps the epoch from the connector config. On the smart snapshot
        // path (a regular initial snapshot, never on-demand) ctx.offset is null, so this builds a fresh one.
        PostgresOffsetContext offset = ctx.offset;
        if (offset == null) {
            offset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock());
            ctx.offset = offset;
        }
        // Overwrite the position with the leader's shared consistent point — the slot LSN and the txId captured
        // there, NOT this task's own WAL position or backend transaction id — so every task's snapshot offset
        // agrees on one consistent point.
        offset.updateWalPosition(smartSnapshotLsn, null, getClock().currentTime(),
                smartSnapshotTxId, null, null, null);
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Set offset LSN={}, txId={}", taskId, epoch, smartSnapshotLsn, smartSnapshotTxId);
    }

    @Override
    protected void setSnapshotTransactionIsolationLevel(boolean isOnDemand) throws SQLException {
        if (smartSnapshotName != null && !isOnDemand) {
            // Reuse the parent's statement builder; only the exported snapshot name differs (leader's, not the slot's).
            String combined = importExportedSnapshotStatement(smartSnapshotName);
            LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Opening snapshot transaction: {}", taskId, epoch, combined);
            jdbcConnection.executeWithoutCommitting(combined);
            return;
        }
        super.setSnapshotTransactionIsolationLevel(isOnDemand);
    }

    @Override
    protected void releaseSchemaSnapshotLocks(
                                              RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext) {
        // Signal transaction_started AFTER schema read (step 5 of doExecute).
        // Same timing as existing single-task releaseSchemaSnapshotLocks().
        // For MySQL: information_schema isn't transactional, so global lock
        // must be held during schema read. Release only after schema is captured.

        // don't catch write failure, let the task fail instead
        snapshotCoordination.writeTaskStartedTransaction(taskId, epoch);
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Signaled task_started_transaction (schema read done)", taskId, epoch);
    }
}
