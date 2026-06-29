/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.connector.postgresql.connection.Lsn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.source.snapshot.SnapshotLifecycleManager;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;

public class PostgresSnapshotLifecycleManager implements SnapshotLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSnapshotLifecycleManager.class);

    private final PostgresConnectorConfig connectorConfig;
    private final MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory;
    private final PostgresTaskContext taskContext;
    private final SnapshotterService snapshotterService;

    // creation replication slot and hold snapshot alive (first start)
    private ReplicationConnection replicationConnection;
    // holds snapshot alive on epoch restart
    private PostgresConnection snapshotHolderConnection;
    // field to hold the metadata connection so we can close it later:
    private PostgresConnection replicationMetadataConnection;

    /*
     * holds ACCESS SHARE locks, this is required when snapshot is held alive via the replication slot
     * as the replication connection can't hold locks
     */
    private PostgresConnection lockHolderConnection;
    private String currentSnapshotName;
    private String currentSlotLsn;

    public PostgresSnapshotLifecycleManager(PostgresConnectorConfig connectorConfig,
                                            MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory,
                                            PostgresTaskContext taskContext, SnapshotterService snapshotterService) {
        this.connectorConfig = connectorConfig;
        this.connectionFactory = connectionFactory;
        this.taskContext = taskContext;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshotSetup prepareSnapshot(List<TableId> tables, boolean shouldStream) {
        if (shouldStream) {
            // Streaming modes (initial, when_needed): need replication slot to pin WAL
            // This logic is similar to the feature disabled path in PostgresConnectorTask#start
            SlotState slotInfo = getSlotState();
            if (slotInfo == null) {
                createSlotViaReplicationProtocol();
            }
            else {
                exportSnapshotFromExistingSlot(slotInfo);
            }
        }
        else {
            // Non-streaming mode (initial_only): just export snapshot, no slot needed
            exportSnapshotWithoutSlot();
        }

        lockAllTables(tables);
        return new SnapshotSetup(currentSnapshotName, currentSlotLsn);
    }

    @Override
    public void onAllTasksJoined() {
        LOGGER.info("Smart snapshot: all tasks joined (Postgres: no lock release needed)");
    }

    @Override
    public String snapshotName() {
        return currentSnapshotName;
    }

    @Override
    public String consistentPosition() {
        return currentSlotLsn;
    }

    private SlotState getSlotState() {
        try (PostgresConnection tempConn = connectionFactory.newConnection()) {
            return tempConn.getReplicationSlotState(
                    connectorConfig.slotName(),
                    connectorConfig.plugin().getPostgresPluginName());
        }
        catch (SQLException e) {
            LOGGER.warn("Smart snapshot: Could not check slot state", e);
            return null;
        }
    }

    private void createSlotViaReplicationProtocol() {
        replicationConnection = createReplicationConnectionWithRetry();

        if (connectorConfig.isReadOnlyConnection()) {
            LOGGER.warn("Smart snapshot: Connector is configured to be in read-only mode but replication slot "
                    + "was not found. The attempt to create it can fail.");
        }

        try {
            SlotCreationResult result = replicationConnection.createReplicationSlot()
                    .orElseThrow(() -> new DebeziumException("Smart snapshot: Slot creation returned no result"));

            currentSlotLsn = result.startLsn().asString();
            currentSnapshotName = result.snapshotName();

            LOGGER.info("Smart snapshot: created slot '{}', LSN={}, snapshot='{}'",
                    connectorConfig.slotName(), currentSlotLsn, currentSnapshotName);
        }
        catch (SQLException ex) {
            releaseSnapshot();
            String message = "Smart snapshot: Creation of replication slot failed";
            if (ex.getMessage() != null && ex.getMessage().contains("already exists")) {
                message += "; when setting up multiple connectors for the same database host, "
                        + "please make sure to use a distinct replication slot name for each.";
            }
            throw new DebeziumException(message, ex);
        }
    }

    // Mirrors PostgresConnectorTask#createReplicationConnection
    private ReplicationConnection createReplicationConnectionWithRetry() {
        replicationMetadataConnection = connectionFactory.newConnection();
        // dropSlotOnClose=false so the slot persists for streaming after downscale
        return PostgresConnectorTask.createReplicationConnectionWithRetry(
                () -> taskContext.createReplicationConnection(replicationMetadataConnection, false),
                connectorConfig.maxRetries(), connectorConfig.retryDelay());
    }

    private void exportSnapshotFromExistingSlot(SlotState slotInfo) {
        try {
            snapshotHolderConnection = connectionFactory.newConnection();
            snapshotHolderConnection.connection().setAutoCommit(false);
            snapshotHolderConnection.executeWithoutCommitting(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            String walBefore = snapshotHolderConnection.queryAndMap(
                    "SELECT pg_current_wal_lsn()::text",
                    snapshotHolderConnection.singleResultMapper(
                            rs -> rs.getString(1), "Failed to get WAL LSN"));

            currentSnapshotName = snapshotHolderConnection.queryAndMap(
                    "SELECT pg_export_snapshot()",
                    snapshotHolderConnection.singleResultMapper(
                            rs -> rs.getString(1), "Failed to export snapshot"));

            if (slotInfo != null && slotInfo.slotLastFlushedLsn() != null) {
                currentSlotLsn = slotInfo.slotLastFlushedLsn().asString();
            }
            else {
                currentSlotLsn = walBefore;
            }
            LOGGER.info("Smart snapshot: exported snapshot '{}', LSN={}",
                    currentSnapshotName, currentSlotLsn);
        }
        catch (SQLException e) {
            releaseSnapshot();
            throw new DebeziumException("Smart snapshot: Failed to export snapshot", e);
        }
    }

    private void exportSnapshotWithoutSlot() {
        try {
            snapshotHolderConnection = connectionFactory.newConnection();
            snapshotHolderConnection.connection().setAutoCommit(false);
            snapshotHolderConnection.executeWithoutCommitting(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            // No slot → no slot LSN. Use current WAL position as reference.
            currentSlotLsn = snapshotHolderConnection.queryAndMap(
                    "SELECT pg_current_wal_lsn()::text",
                    snapshotHolderConnection.singleResultMapper(
                            rs -> rs.getString(1), "Failed to get WAL LSN"));

            currentSnapshotName = snapshotHolderConnection.queryAndMap(
                    "SELECT pg_export_snapshot()",
                    snapshotHolderConnection.singleResultMapper(
                            rs -> rs.getString(1), "Failed to export snapshot"));

            LOGGER.info("Smart snapshot: (no-stream) exported snapshot '{}', LSN={}",
                    currentSnapshotName, currentSlotLsn);
        }
        catch (SQLException e) {
            releaseSnapshot();
            throw new DebeziumException("Smart snapshot: Failed to export snapshot", e);
        }
    }

    // Mirrors PostgresSnapshotChangeEventSource.lockTablesForSchemaSnapshot()
    private void lockAllTables(List<TableId> tables) {
        // Use the configured snapshot lock strategy (shared/none) and lock timeout
        try {
            lockHolderConnection = connectionFactory.newConnection();
            lockHolderConnection.connection().setAutoCommit(false);

            Duration lockTimeout = connectorConfig.snapshotLockTimeout();
            String lineSeparator = System.lineSeparator();
            StringBuilder statements = new StringBuilder();
            statements.append("SET lock_timeout = ").append(lockTimeout.toMillis())
                    .append(";").append(lineSeparator);
            List<String> lockStatements = tables.stream()
                    .map(t -> snapshotterService.getSnapshotLock().tableLockingStatement(lockTimeout, t.toDoubleQuotedString()))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());

            if (lockStatements.isEmpty()) {
                LOGGER.info("Smart snapshot: no table locking (snapshot.locking.mode=none)");
                return;
            }

            lockStatements.forEach(tableStatement -> statements.append(tableStatement).append(lineSeparator));

            LOGGER.info("Smart snapshot: locking {} tables (timeout={}s)",
                    tables.size(), lockTimeout.getSeconds());
            // we're locking in ACCESS SHARE MODE to avoid concurrent schema changes while we're taking the snapshot
            // this does not prevent writes to the table, but prevents changes to the table's schema....
            // DBZ-298 Quoting name in case it has been quoted originally; it doesn't do harm if it hasn't been quoted
            lockHolderConnection.executeWithoutCommitting(statements.toString());
        }
        catch (SQLException e) {
            if (lockHolderConnection != null) {
                try {
                    lockHolderConnection.close();
                }
                catch (Exception ignored) {
                }
                lockHolderConnection = null;
            }
            throw new DebeziumException("Smart snapshot: Failed to lock tables", e);
        }
    }

    @Override
    public void keepAlive() {
        pingOrThrow(snapshotHolderConnection, "snapshot-holder");
        pingOrThrow(lockHolderConnection, "lock-holder");
        ReplicationConnection rc = this.replicationConnection;
        if (rc != null) {
            try {
                if (!rc.isConnected()) {
                    throw new DebeziumException("Smart snapshot: replication connection is no longer connected");
                }
            }
            catch (SQLException e) {
                throw new DebeziumException("Smart snapshot: replication connection liveness check failed", e);
            }
        }
    }

    private void pingOrThrow(PostgresConnection conn, String which) {
        if (conn == null) {
            return;
        }
        try {
            // runs inside the open snapshot/lock transaction — harmless, resets idle timer
            conn.executeWithoutCommitting("SELECT 1");
        }
        catch (SQLException e) {
            throw new DebeziumException("Smart snapshot: " + which + " connection is dead (held snapshot lost)", e);
        }
    }

    @Override
    public void releaseSnapshot() {
        ReplicationConnection replicationConn = this.replicationConnection;
        this.replicationConnection = null;
        if (replicationConn != null) {
            try {
                replicationConn.close();
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot: Error closing replication connection", e);
            }
        }
        PostgresConnection snapshotHolderConn = this.snapshotHolderConnection;
        this.snapshotHolderConnection = null;
        if (snapshotHolderConn != null) {
            try {
                snapshotHolderConn.close();
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot: Error closing snapshot holder connection", e);
            }
        }
        PostgresConnection lockHolderConn = this.lockHolderConnection;
        this.lockHolderConnection = null;
        if (lockHolderConn != null) {
            try {
                lockHolderConn.close();
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot: Error closing lock holder connection", e);
            }
        }
        PostgresConnection replicationMetadataConn = this.replicationMetadataConnection;
        this.replicationMetadataConnection = null;
        if (replicationMetadataConn != null) {
            try {
                replicationMetadataConn.close();
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot: Error closing replication metadata connection", e);
            }
        }
        currentSnapshotName = null;
        currentSlotLsn = null;
    }
}
