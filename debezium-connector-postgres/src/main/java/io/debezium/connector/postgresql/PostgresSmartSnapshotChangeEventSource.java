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

    private final PostgresConnectorConfig connectorConfig;
    private final PostgresConnection jdbcConnection;
    private final String taskId;

    private volatile SnapshotCoordinationFacade snapshotCoordination;
    private volatile int epoch;
    private volatile String smartSnapshotName;
    private volatile Lsn smartSnapshotLsn;
    private volatile List<TableId> smartSnapshotTables;

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
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
        this.taskId = connectorConfig.getTaskId();
    }

    public void setSnapshotCoordination(
                                        int epoch,
                                        String snapshotName,
                                        Lsn lsn,
                                        List<TableId> tableIds,
                                        SnapshotCoordinationFacade coordination) {
        this.epoch = epoch;
        this.smartSnapshotName = snapshotName;
        this.smartSnapshotLsn = lsn;
        this.smartSnapshotTables = tableIds;
        this.snapshotCoordination = coordination;
    }

    @Override
    protected void determineCapturedTables(
                                           RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx,
                                           Set<Pattern> ignoredSnapshotPatterns, SnapshottingTask snapshottingTask) {
        // this task's slice is already the final, filtered, sorted set from the leader; snapshot exactly it.
        // signaling collection is assigned to one task via the split -> snapshotted once (no per-task re-add).
        LinkedHashSet<TableId> mine = new LinkedHashSet<>(smartSnapshotTables);
        ctx.capturedTables = mine;
        ctx.capturedSchemaTables = mine; // unused on the Postgres path (readTableStructure derives schemas from capturedTables)
        // todo should we log each tableId?
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Determining captured tables using the slice from the leader", taskId, epoch);
    }

    @Override
    protected void determineSnapshotOffset(
                                           RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx,
                                           PostgresOffsetContext previousOffset)
            throws Exception {
        // Create fresh offset with the Connector's slot LSN — not current WAL position
        PostgresOffsetContext offset = PostgresOffsetContext.initialContext(
                connectorConfig, jdbcConnection, getClock());
        Long txId = jdbcConnection.currentTransactionId();
        offset.updateWalPosition(smartSnapshotLsn, null, getClock().currentTime(),
                txId, null, null, null);
        ctx.offset = offset;
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Set offset LSN={}", taskId, epoch, smartSnapshotLsn);
    }

    @Override
    protected void setSnapshotTransactionIsolationLevel(boolean isOnDemand) throws SQLException {
        if (smartSnapshotName != null && !isOnDemand) {
            String snapSet = String.format("SET TRANSACTION SNAPSHOT '%s';", smartSnapshotName);
            String combined = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; \n" + snapSet;
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
        snapshotCoordination.writeTransactionStarted(taskId, epoch);
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Signaled transaction_started (schema read done)", taskId, epoch);
    }
}
