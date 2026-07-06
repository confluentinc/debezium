/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

public class PostgresSmartSnapshotLifecycleManager implements SmartSnapshotLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSmartSnapshotLifecycleManager.class);

    // PostgresLeaderSchemaSource#discoverAndLock only needs isRunning()==true; everything else is a harmless stub.
    private static final ChangeEventSource.ChangeEventSourceContext RUNNING_CONTEXT = new ChangeEventSource.ChangeEventSourceContext() {
        @Override
        public boolean isPaused() {
            return false;
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public void resumeStreaming() {

        }

        @Override
        public void waitSnapshotCompletion() {

        }

        @Override
        public void streamingPaused() {

        }

        @Override
        public void waitStreamingPaused() {

        }
    };

    private final PostgresConnectorConfig connectorConfig;
    private final MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory;
    private final PostgresTaskContext taskContext;
    private final SnapshotterService snapshotterService;

    // needed specifically to build inner class PostgresSmartSnapshotLeaderSchemaSource
    private final PostgresSchema schema;
    private final EventDispatcher<PostgresPartition, TableId> dispatcher;
    private final NotificationService<PostgresPartition, PostgresOffsetContext> notificationService;
    private final Clock clock;

    // New-slot path only. Runs CREATE_REPLICATION_SLOT (dropSlotOnClose=false, so the slot persists for
    // streaming) and holds the slot's exported snapshot alive until every task has imported it.
    private ReplicationConnection replicationConnection;

    // JDBC companion the replication builder needs (type registry / schema metadata). No snapshot role of its
    // own — retained only so it can be closed alongside replicationConnection on release.
    private PostgresConnection replicationMetadataConnection;

    // Existing-slot / no-slot paths. Runs pg_export_snapshot() inside a REPEATABLE READ txn to produce the
    // shared snapshot; must stay open, since the exported snapshot is only valid while this txn lives.
    private PostgresConnection snapshotHolderConnection;

    // The leader's schema-snapshot connection: imports the shared snapshot (SET TRANSACTION SNAPSHOT),
    // discovers the tables under it, and holds ACCESS SHARE (if configured via snapshot.locking.mode) on them.
    // Kept open until onAllTasksJoined / release.
    private PostgresConnection schemaSnapshotConnection;

    public PostgresSmartSnapshotLifecycleManager(PostgresConnectorConfig connectorConfig,
                                                 MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory,
                                                 PostgresTaskContext taskContext, SnapshotterService snapshotterService,
                                                 PostgresSchema schema, EventDispatcher<PostgresPartition, TableId> dispatcher,
                                                 NotificationService<PostgresPartition, PostgresOffsetContext> notificationService, Clock clock) {
        this.connectorConfig = connectorConfig;
        this.connectionFactory = connectionFactory;
        this.taskContext = taskContext;
        this.snapshotterService = snapshotterService;

        // needed specifically to build inner class PostgresSmartSnapshotLeaderSchemaSource
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.notificationService = notificationService;
        this.clock = clock;
    }

    /**
     * These three methods share the held connections and can be called from two threads at once (the leader
     * prep thread and the task-stop path). 'synchronized' runs them one at a time so a release can't close a
     * connection that keepAlive/prepare is still using. It's reentrant, so prepareSnapshot's error-path
     * releaseSnapshot() still works.
     */
    @Override
    public synchronized SnapshotSetup prepareSnapshot(boolean shouldStream) {
        SlotCreateOrExportResult slotCreateOrExportResult;

        if (shouldStream) {
            // Streaming modes (initial, when_needed): need replication slot to pin WAL
            // This logic is similar to the feature disabled path in PostgresConnectorTask#start
            SlotState slotInfo = getSlotState();
            if (slotInfo == null) {
                LOGGER.warn("Smart snapshot: Unable to load info of replication slot, Debezium will try to create the slot");
                if (connectorConfig.isReadOnlyConnection()) {
                    LOGGER.warn("Connector is configured to be in read-only mode but replication slot was not found.\n" +
                            "The attempt to create it can fail. Please check you configuration in case.");
                }
                slotCreateOrExportResult = createSlotViaReplicationProtocol();
            }
            else {
                slotCreateOrExportResult = exportSnapshotFromExistingSlot(slotInfo);
            }
        }
        else {
            // Non-streaming mode (initial_only): just export snapshot, no slot needed
            slotCreateOrExportResult = exportSnapshotWithoutSlot();
        }

        try {
            this.schemaSnapshotConnection = connectionFactory.newConnection();
            // so executeWithoutCommitting HOLDS the locks
            this.schemaSnapshotConnection.connection().setAutoCommit(false);
            MainConnectionProvidingConnectionFactory<PostgresConnection> heldFactory = new MainConnectionProvidingConnectionFactory<>() {
                public PostgresConnection mainConnection() {
                    return schemaSnapshotConnection;
                }

                public PostgresConnection newConnection() {
                    return connectionFactory.newConnection();
                }
            };

            // leader schema source -> imports currentSnapshotName EXPLICITLY on `schemaSnapshotConnection` (works for
            // new AND existing slot), discovers UNDER it, locks. Pass currentSnapshotName as exportedSnapshotName.
            PostgresLeaderSchemaSource leaderSource = new PostgresLeaderSchemaSource(
                    connectorConfig, snapshotterService, heldFactory, schema, dispatcher, clock, notificationService,
                    slotCreateOrExportResult.getSlotCreationResult(), slotCreateOrExportResult.getStartingSlotState(),
                    slotCreateOrExportResult.getCurrentSnapshotName());
            PostgresPartition leaderPartition = new PostgresPartition(connectorConfig.getConnectorName(), "", "0");
            List<TableId> tables = leaderSource.discoverAndLock(leaderPartition, RUNNING_CONTEXT);
            return new SnapshotSetup(slotCreateOrExportResult.getCurrentSnapshotName(), slotCreateOrExportResult.getCurrentSlotLsn(), tables);
        }
        catch (Exception e) {
            releaseSnapshot();
            throw new DebeziumException("Smart snapshot: [Leader] Failed to import/discover/lock the exported snapshot", e);
        }
    }

    @Override
    public void onAllTasksJoined() {
        LOGGER.info("Smart snapshot: [Leader] All tasks joined.");
        releaseSnapshot();
    }

    private SlotState getSlotState() {
        try (PostgresConnection tempConn = connectionFactory.newConnection()) {
            return tempConn.getReplicationSlotState(
                    connectorConfig.slotName(),
                    connectorConfig.plugin().getPostgresPluginName());
        }
        catch (SQLException e) {
            LOGGER.warn("Smart snapshot: [Leader] Could not check slot state", e);
            return null;
        }
    }

    private SlotCreateOrExportResult createSlotViaReplicationProtocol() {
        replicationConnection = createReplicationConnectionWithRetry();

        if (connectorConfig.isReadOnlyConnection()) {
            LOGGER.warn("Smart snapshot: [Leader] Connector is configured to be in read-only mode but replication slot "
                    + "was not found. The attempt to create it can fail.");
        }

        try {
            SlotCreationResult result = replicationConnection.createReplicationSlot()
                    .orElseThrow(() -> new DebeziumException("Smart snapshot: [Leader] Slot creation returned no result"));

            String currentSlotLsn = result.startLsn().asString();
            String currentSnapshotName = result.snapshotName();

            LOGGER.info("Smart snapshot: [Leader] Created slot '{}', LSN={}, snapshot='{}'",
                    connectorConfig.slotName(), currentSlotLsn, currentSnapshotName);

            return new SlotCreateOrExportResult(result, null, currentSnapshotName, currentSlotLsn);
        }
        catch (SQLException ex) {
            releaseSnapshot();
            String message = "Smart snapshot: [Leader] Creation of replication slot failed";
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

    private SlotCreateOrExportResult exportSnapshotFromExistingSlot(SlotState slotInfo) {
        try {
            snapshotHolderConnection = connectionFactory.newConnection();
            snapshotHolderConnection.connection().setAutoCommit(false);
            snapshotHolderConnection.executeWithoutCommitting(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            String walBefore = snapshotHolderConnection.queryAndMap(
                    "SELECT pg_current_wal_lsn()::text",
                    snapshotHolderConnection.singleResultMapper(
                            rs -> rs.getString(1), "Failed to get WAL LSN"));

            String currentSnapshotName = snapshotHolderConnection.queryAndMap(
                    "SELECT pg_export_snapshot()",
                    snapshotHolderConnection.singleResultMapper(
                            rs -> rs.getString(1), "Failed to export snapshot"));

            String currentSlotLsn;
            if (slotInfo != null && slotInfo.slotLastFlushedLsn() != null) {
                currentSlotLsn = slotInfo.slotLastFlushedLsn().asString();
            }
            else {
                currentSlotLsn = walBefore;
            }
            LOGGER.info("Smart snapshot: [Leader] exported snapshot '{}', LSN={}",
                    currentSnapshotName, currentSlotLsn);

            return new SlotCreateOrExportResult(null, slotInfo, currentSnapshotName, currentSlotLsn);
        }
        catch (SQLException e) {
            releaseSnapshot();
            throw new DebeziumException("Smart snapshot: Failed to export snapshot", e);
        }
    }

    private SlotCreateOrExportResult exportSnapshotWithoutSlot() {
        try {
            snapshotHolderConnection = connectionFactory.newConnection();
            snapshotHolderConnection.connection().setAutoCommit(false);
            snapshotHolderConnection.executeWithoutCommitting(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            // No slot → no slot LSN. Use current WAL position as reference.
            String currentSlotLsn = snapshotHolderConnection.queryAndMap(
                    "SELECT pg_current_wal_lsn()::text",
                    snapshotHolderConnection.singleResultMapper(
                            rs -> rs.getString(1), "Failed to get WAL LSN"));

            String currentSnapshotName = snapshotHolderConnection.queryAndMap(
                    "SELECT pg_export_snapshot()",
                    snapshotHolderConnection.singleResultMapper(
                            rs -> rs.getString(1), "Failed to export snapshot"));

            LOGGER.info("Smart snapshot: [Leader] (no-stream) exported snapshot '{}', LSN={}",
                    currentSnapshotName, currentSlotLsn);

            return new SlotCreateOrExportResult(null, null, currentSnapshotName, currentSlotLsn);
        }
        catch (SQLException e) {
            releaseSnapshot();
            throw new DebeziumException("Smart snapshot: [Leader] Failed to export snapshot", e);
        }
    }

    @Override
    public synchronized void keepAlive() {
        pingOrThrow(snapshotHolderConnection, "snapshot-holder");
        pingOrThrow(schemaSnapshotConnection, "schema-snapshot-holder");
        ReplicationConnection replicationConnectionCopy = this.replicationConnection;
        if (replicationConnectionCopy != null) {
            try {
                if (!replicationConnectionCopy.isConnected()) {
                    throw new DebeziumException("Smart snapshot: [Leader] replication connection is no longer connected");
                }
            }
            catch (SQLException e) {
                throw new DebeziumException("Smart snapshot: [Leader] replication connection liveness check failed", e);
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
            throw new DebeziumException("Smart snapshot: [Leader] " + which + " connection is dead (held snapshot lost)", e);
        }
    }

    @Override
    public synchronized void releaseSnapshot() {
        ReplicationConnection replicationConn = this.replicationConnection;
        this.replicationConnection = null;
        if (replicationConn != null) {
            try {
                replicationConn.close();
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot: [Leader] Error closing replication connection", e);
            }
        }
        PostgresConnection snapshotHolderConn = this.snapshotHolderConnection;
        this.snapshotHolderConnection = null;
        if (snapshotHolderConn != null) {
            try {
                snapshotHolderConn.close();
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot: [Leader] Error closing snapshot holder connection", e);
            }
        }
        PostgresConnection lockHolderConn = this.schemaSnapshotConnection;
        this.schemaSnapshotConnection = null;
        if (lockHolderConn != null) {
            try {
                lockHolderConn.close();
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot: [Leader] Error closing lock holder connection", e);
            }
        }
        PostgresConnection replicationMetadataConn = this.replicationMetadataConnection;
        this.replicationMetadataConnection = null;
        if (replicationMetadataConn != null) {
            try {
                replicationMetadataConn.close();
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot: [Leader] Error closing replication metadata connection", e);
            }
        }
    }

    static class PostgresLeaderSchemaSource extends PostgresSnapshotChangeEventSource {
        private final PostgresConnection connection;
        // the leader's pg_export_snapshot / new-slot exported name
        private final String exportedSnapshotName;

        PostgresLeaderSchemaSource(PostgresConnectorConfig connectorConfig, SnapshotterService snapshotterService,
                                   MainConnectionProvidingConnectionFactory<PostgresConnection> heldConnectionFactory,
                                   PostgresSchema schema, EventDispatcher<PostgresPartition, TableId> dispatcher, Clock clock,
                                   NotificationService<PostgresPartition, PostgresOffsetContext> notificationService,
                                   SlotCreationResult slotCreatedInfo, SlotState startingSlotInfo,
                                   // exported name -> import it EXPLICITLY (works for new AND existing slot)
                                   String exportedSnapshotName) {
            super(connectorConfig, snapshotterService, heldConnectionFactory, schema, dispatcher, clock, SnapshotProgressListener.NO_OP(),
                    slotCreatedInfo, startingSlotInfo, notificationService);
            this.connection = heldConnectionFactory.mainConnection();
            this.exportedSnapshotName = exportedSnapshotName;
        }

        /**
         * Postgres lets one session capture a point-in-time and name it -- via pg_export_snapshot(), or as a
         * side effect of creating a replication slot. Another session joins that exact same point-in-time by
         * starting its transaction with:  SET TRANSACTION SNAPSHOT '<name>';   (that join step is "importing").
         * The leader must read at this shared point so the tables it discovers and locks are exactly the ones
         * the tasks snapshot.
         * <p>
         * exportedSnapshotName is that shared snapshot's name, produced one of two ways:
         * 1. the replication slot was just created -> creating the slot exported it, or
         * 2. the slot already existed (or there is no slot) -> the leader ran pg_export_snapshot() itself.
         * <p>
         * The normal single-task snapshot code only issues SET TRANSACTION SNAPSHOT in case 1. In case 2it
         * starts a plain transaction with no SET TRANSACTION SNAPSHOT, so the session reads at its OWN,later
         * point-in-time -- OK for a lone reader, but wrong for the leader and multiple followers, which must match the tasks.
         * So we always issue it here:  SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;  SET TRANSACTION SNAPSHOT '<name>';
         */
        @Override
        protected void setSnapshotTransactionIsolationLevel(boolean isOnDemand) throws SQLException {
            if (exportedSnapshotName != null && !isOnDemand) {
                LOGGER.info("Smart snapshot: [Leader] Setting transaction isolation level on the exported snapshot {}", exportedSnapshotName);
                String combined = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; \n"
                        + String.format("SET TRANSACTION SNAPSHOT '%s';", exportedSnapshotName);
                connection.executeWithoutCommitting(combined);
                return;
            }
            super.setSnapshotTransactionIsolationLevel(isOnDemand);
        }

        /**
         * Import the exported snapshot on the held connection, discover UNDER it, then lock it (held, no commit).
         */
        List<TableId> discoverAndLock(PostgresPartition partition, ChangeEventSourceContext running) throws Exception {
            SnapshottingTask task = getSnapshottingTask(partition, null);
            RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx = (RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext>) prepare(
                    partition, false);
            // REUSE -> setSnapshotTransactionIsolationLevel (overridden above) -> imports exported name on `held`
            connectionCreated(ctx);
            LOGGER.info("Smart snapshot: [Leader] Determining captured tables.");
            // discover UNDER the snapshot
            determineCapturedTables(ctx, getDataCollectionPattern(task.getDataCollections()), task);
            LOGGER.info("Smart snapshot: [Leader] Optionally locking tables for schema snapshot.");
            // lock (according to snapshot.locking.mode)
            lockTablesForSchemaSnapshot(running, ctx);
            return new ArrayList<>(ctx.capturedTables);
        }
    }

    static class SlotCreateOrExportResult {
        private final SlotCreationResult slotCreationResult;
        private final SlotState startingSlotState;
        private final String currentSnapshotName;
        private final String currentSlotLsn;

        public SlotCreateOrExportResult(SlotCreationResult slotCreationResult, SlotState startingSlotState, String currentSnapshotName, String currentSlotLsn) {
            this.slotCreationResult = slotCreationResult;
            this.startingSlotState = startingSlotState;
            this.currentSnapshotName = currentSnapshotName;
            this.currentSlotLsn = currentSlotLsn;
        }

        public SlotCreationResult getSlotCreationResult() {
            return slotCreationResult;
        }

        public SlotState getStartingSlotState() {
            return startingSlotState;
        }

        public String getCurrentSnapshotName() {
            return currentSnapshotName;
        }

        public String getCurrentSlotLsn() {
            return currentSlotLsn;
        }
    }
}
