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

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.pipeline.source.snapshot.SnapshotLifecycleManager;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.ThreadNameContext;

public class PostgresSnapshotLifecycleManager implements SnapshotLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSnapshotLifecycleManager.class);

    private final PostgresConnectorConfig connectorConfig;
    private final ThreadNameContext threadNameContext;
    private final SnapshotterService snapshotterService;

    // creation replication slot and hold snapshot alive (first start)
    private ReplicationConnection replicationConnection;
    // holds snapshot alive on epoch restart
    private PostgresConnection snapshotHolderConnection;

    /*
     * holds ACCESS SHARE locks, this is required when snapshot is held alive via the replication slot
     * as the replication connection can't hold locks
     */
    private PostgresConnection lockHolderConnection;
    private String currentSnapshotName;
    private String currentSlotLsn;

    public PostgresSnapshotLifecycleManager(PostgresConnectorConfig connectorConfig,
                                            ThreadNameContext threadNameContext,
                                            SnapshotterService snapshotterService) {
        this.connectorConfig = connectorConfig;
        this.threadNameContext = threadNameContext;
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
    public boolean requiresFullRestartOnTaskFailure() {
        // Tasks can rejoin via SET TRANSACTION SNAPSHOT, the connector holds access share lock for the entire duration
        return false;
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
            lockHolderConn = null;
        }
        currentSnapshotName = null;
        currentSlotLsn = null;
    }

    @Override
    public boolean isValid() {
        // Check whichever connection holds the snapshot alive
        ReplicationConnection replicationConn = this.replicationConnection;
        if (replicationConn != null) {
            try {
                return replicationConn.isConnected();
            }
            catch (SQLException e) {
                return false;
            }
        }
        PostgresConnection holder = this.snapshotHolderConnection;
        if (holder != null) {
            try {
                holder.execute("SELECT 1");
                return true;
            }
            catch (SQLException e) {
                return false;
            }
        }
        return false;
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
        try (PostgresConnection tempConn = new PostgresConnection(connectorConfig.getJdbcConfig(),
                PostgresConnection.CONNECTION_GENERAL, threadNameContext)) {
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

    // this is similar to PostgresConnectorTask#createReplicationConnection
    private ReplicationConnection createReplicationConnectionWithRetry() {
        PostgresConnection metadataConnection = new PostgresConnection(
                connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL,
                threadNameContext);

        final Metronome metronome = Metronome.parker(connectorConfig.retryDelay(), Clock.SYSTEM);
        short retryCount = 0;
        final int maxRetries = connectorConfig.maxRetries();

        while (retryCount <= maxRetries) {
            try {
                return ReplicationConnection.builder(connectorConfig)
                        .withSlot(connectorConfig.slotName())
                        .withPublication(connectorConfig.publicationName())
                        .withTableFilter(connectorConfig.getTableFilters())
                        .withPublicationAutocreateMode(connectorConfig.publicationAutocreateMode())
                        .withPlugin(connectorConfig.plugin())
                        .dropSlotOnClose(false)
                        .createFailOverSlot(connectorConfig.createFailOverSlot())
                        .streamParams(connectorConfig.streamParams())
                        .statusUpdateInterval(connectorConfig.statusUpdateInterval())
                        .withTypeRegistry(metadataConnection.getTypeRegistry())
                        .withSchema(null)
                        .jdbcMetadataConnection(metadataConnection)
                        .build();
            }
            catch (Exception ex) {
                retryCount++;
                if (retryCount > maxRetries) {
                    LOGGER.error("Smart snapshot: Too many errors connecting to server. All {} retries failed.",
                            maxRetries);
                    throw new ConnectException(ex.getMessage());
                }
                LOGGER.warn("Smart snapshot: Error connecting to server; will attempt retry {} of {} after {} "
                        + "seconds. Exception message: {}",
                        retryCount, maxRetries, connectorConfig.retryDelay().getSeconds(),
                        ex.getMessage());
                try {
                    metronome.pause();
                }
                catch (InterruptedException e) {
                    LOGGER.warn("Smart snapshot: Connection retry sleep interrupted by exception: " + e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new ConnectException("Smart snapshot: Failed to create replication connection");
    }

    private void exportSnapshotFromExistingSlot(SlotState slotInfo) {
        try {
            snapshotHolderConnection = new PostgresConnection(connectorConfig.getJdbcConfig(),
                    PostgresConnection.CONNECTION_GENERAL, threadNameContext);
            snapshotHolderConnection.connection().setAutoCommit(false);
            snapshotHolderConnection.executeWithoutCommitting(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            currentSnapshotName = snapshotHolderConnection.queryAndMap(
                    "SELECT pg_export_snapshot()",
                    snapshotHolderConnection.singleResultMapper(
                            rs -> rs.getString(1), "Failed to export snapshot"));

            if (slotInfo != null && slotInfo.slotLastFlushedLsn() != null) {
                currentSlotLsn = slotInfo.slotLastFlushedLsn().asString();
            }
            else {
                currentSlotLsn = snapshotHolderConnection.queryAndMap(
                        "SELECT pg_current_wal_lsn()::text",
                        snapshotHolderConnection.singleResultMapper(
                                rs -> rs.getString(1), "Failed to get WAL LSN"));
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
            snapshotHolderConnection = new PostgresConnection(connectorConfig.getJdbcConfig(),
                    PostgresConnection.CONNECTION_GENERAL, threadNameContext);
            snapshotHolderConnection.connection().setAutoCommit(false);
            snapshotHolderConnection.executeWithoutCommitting(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            currentSnapshotName = snapshotHolderConnection.queryAndMap(
                    "SELECT pg_export_snapshot()",
                    snapshotHolderConnection.singleResultMapper(
                            rs -> rs.getString(1), "Failed to export snapshot"));

            // No slot → no slot LSN. Use current WAL position as reference.
            currentSlotLsn = snapshotHolderConnection.queryAndMap(
                    "SELECT pg_current_wal_lsn()::text",
                    snapshotHolderConnection.singleResultMapper(
                            rs -> rs.getString(1), "Failed to get WAL LSN"));

            LOGGER.info("Smart snapshot: (no-stream) exported snapshot '{}', LSN={}",
                    currentSnapshotName, currentSlotLsn);
        }
        catch (SQLException e) {
            releaseSnapshot();
            throw new DebeziumException("Smart snapshot: Failed to export snapshot", e);
        }
    }

    // similar to PostgresSnapshotChangeEventSource.lockTablesForSchemaSnapshot
    private void lockAllTables(List<TableId> tables) {
        // Use the configured snapshot lock strategy (shared/none) and lock timeout
        // Mirrors PostgresSnapshotChangeEventSource.lockTablesForSchemaSnapshot()
        final Duration lockTimeout = connectorConfig.snapshotLockTimeout();
        List<String> lockStatements = tables.stream()
                .map(t -> snapshotterService.getSnapshotLock()
                        .tableLockingStatement(lockTimeout, t.toDoubleQuotedString()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        if (lockStatements.isEmpty()) {
            LOGGER.info("Smart snapshot: no table locking configured (snapshot.locking.mode=none)");
            return;
        }

        try {
            lockHolderConnection = new PostgresConnection(connectorConfig.getJdbcConfig(),
                    PostgresConnection.CONNECTION_GENERAL, threadNameContext);
            lockHolderConnection.connection().setAutoCommit(false);

            // Build single combined statement — same as existing snapshot code
            String lineSeparator = System.lineSeparator();
            StringBuilder statements = new StringBuilder();
            statements.append("SET lock_timeout = ").append(lockTimeout.toMillis())
                    .append(";").append(lineSeparator);
            lockStatements.forEach(stmt -> statements.append(stmt).append(lineSeparator));

            LOGGER.info("Smart snapshot: locking {} tables (timeout={}s)",
                    lockStatements.size(), lockTimeout.getSeconds());
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
}
