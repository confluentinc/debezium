/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

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
    private final Integer epoch;

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
    // Kept open until onAllTasksTransactionStarted / release.
    private PostgresConnection schemaSnapshotConnection;

    // Once true, prepareSnapshot() must not retain any newly opened connection.
    private boolean released;

    // Guards the four held-connection references above and the 'released' flag. Held only for the brief
    // reference read/write and never across a JDBC round-trip, so releaseSnapshot() (task-stop thread) can run
    // while prepareSnapshot() (leader prep thread) is blocked in a non-interruptible JDBC call and abort it by
    // closing the connections.
    private final ReentrantLock stateLock = new ReentrantLock();

    public PostgresSmartSnapshotLifecycleManager(PostgresConnectorConfig connectorConfig,
                                                 MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory,
                                                 PostgresTaskContext taskContext, SnapshotterService snapshotterService,
                                                 PostgresSchema schema, EventDispatcher<PostgresPartition, TableId> dispatcher,
                                                 NotificationService<PostgresPartition, PostgresOffsetContext> notificationService,
                                                 Clock clock, Integer epoch) {
        this.connectorConfig = connectorConfig;
        this.connectionFactory = connectionFactory;
        this.taskContext = taskContext;
        this.snapshotterService = snapshotterService;
        this.epoch = epoch;

        // needed specifically to build inner class PostgresSmartSnapshotLeaderSchemaSource
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.notificationService = notificationService;
        this.clock = clock;
    }

    /**
     * prepareSnapshot, keepAlive and releaseSnapshot methods share the held connections and may be
     * called from two threads at the same time: the leader thread and the task-stop
     * thread. The old design made all three methods 'synchronized'. That serialised the long,
     * database-bound prepare against release, so a task stop could be blocked until prepare finished.
     * <p>
     * Instead, only the connection references and the 'released' flag are guarded by
     * {@link #stateLock}. prepareSnapshot runs its database work without the lock and records
     * each connection through {@link #register}. releaseSnapshot sets 'released', detaches the
     * references, then closes them, which stops any prepare still in progress.
     */
    @Override
    public SnapshotSetup prepareSnapshot(boolean shouldStream) {
        SlotCreateOrExportResult slotCreateOrExportResult;

        if (shouldStream) {
            // Streaming modes (initial, when_needed): need replication slot to pin WAL
            // This logic is similar to the feature disabled path in PostgresConnectorTask#start
            SlotState slotInfo = getSlotState();
            if (slotInfo == null) {
                LOGGER.warn("Smart snapshot: [role=leader epoch={}] Unable to load info of replication slot, Debezium will try to create the slot", epoch);
                if (connectorConfig.isReadOnlyConnection()) {
                    LOGGER.warn("Smart snapshot: [role=leader epoch={}] Connector is configured to be in read-only mode but replication slot "
                            + "was not found. The attempt to create it can fail. Please check your configuration", epoch);
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
            // Work through the local 'holder' reference, not the field. register() sets the field so
            // releaseSnapshot() can find and close this connection; but if a release happens while
            // this method is still running, the field is set to null. Reading the field afterwards
            // would then throw a NullPointerException, whereas the local always points to this
            // connection. If a release closes it underneath us, the next call on the local fails with
            // a normal "connection closed" SQLException, which is caught and treated as a shutdown.
            final PostgresConnection holder = connectionFactory.newConnection();
            register(holder, () -> this.schemaSnapshotConnection = holder);
            // set auto-commit off so executeWithoutCommitting holds the locks
            holder.connection().setAutoCommit(false);
            MainConnectionProvidingConnectionFactory<PostgresConnection> heldFactory = new MainConnectionProvidingConnectionFactory<>() {
                public PostgresConnection mainConnection() {
                    return holder;
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
                    slotCreateOrExportResult.getCurrentSnapshotName(), epoch);
            PostgresPartition leaderPartition = new PostgresPartition(connectorConfig.getConnectorName(), "", "0");
            List<TableId> tables = leaderSource.discoverAndLock(leaderPartition, RUNNING_CONTEXT);
            return new SnapshotSetup(slotCreateOrExportResult.getCurrentSnapshotName(), slotCreateOrExportResult.getCurrentSlotLsn(), tables);
        }
        catch (Exception e) {
            releaseSnapshot();
            throw new DebeziumException("Smart snapshot: [role=leader epoch=" + epoch + "] Failed to import/discover/lock the exported snapshot", e);
        }
    }

    @Override
    public void onAllTasksStartedTransaction() {
        LOGGER.info("Smart snapshot: [role=leader epoch={}] All tasks started their transaction", epoch);
        releaseSnapshot();
    }

    private SlotState getSlotState() {
        try (PostgresConnection tempConn = connectionFactory.newConnection()) {
            return tempConn.getReplicationSlotState(
                    connectorConfig.slotName(),
                    connectorConfig.plugin().getPostgresPluginName());
        }
        catch (SQLException e) {
            LOGGER.warn("Smart snapshot: [role=leader epoch={}] Could not check slot state", epoch, e);
            return null;
        }
    }

    private SlotCreateOrExportResult createSlotViaReplicationProtocol() {
        final ReplicationConnection replConn = createReplicationConnectionWithRetry();
        register(replConn, () -> this.replicationConnection = replConn);

        if (connectorConfig.isReadOnlyConnection()) {
            LOGGER.warn("Smart snapshot: [role=leader epoch={}] Connector is configured to be in read-only mode but replication slot "
                    + "was not found. The attempt to create it can fail", epoch);
        }

        try {
            SlotCreationResult result = replConn.createReplicationSlot()
                    .orElseThrow(() -> new DebeziumException("Smart snapshot: [role=leader epoch=" + epoch + "] Slot creation returned no result"));

            String currentSlotLsn = result.startLsn().asString();
            String currentSnapshotName = result.snapshotName();

            LOGGER.info("Smart snapshot: [role=leader epoch={}] Created slot={}, LSN={}, snapshot={}",
                    epoch, connectorConfig.slotName(), currentSlotLsn, currentSnapshotName);

            return new SlotCreateOrExportResult(result, null, currentSnapshotName, currentSlotLsn);
        }
        catch (SQLException ex) {
            releaseSnapshot();
            String message = "Smart snapshot: [role=leader epoch=" + epoch + "] Creation of replication slot failed";
            if (ex.getMessage() != null && ex.getMessage().contains("already exists")) {
                message += "; when setting up multiple connectors for the same database host, "
                        + "please make sure to use a distinct replication slot name for each.";
            }
            throw new DebeziumException(message, ex);
        }
    }

    // Mirrors PostgresConnectorTask#createReplicationConnection
    private ReplicationConnection createReplicationConnectionWithRetry() {
        final PostgresConnection metadataConn = connectionFactory.newConnection();
        register(metadataConn, () -> this.replicationMetadataConnection = metadataConn);
        // dropSlotOnClose=false so the slot persists for streaming after downscale
        return PostgresConnectorTask.createReplicationConnectionWithRetry(
                () -> taskContext.createReplicationConnection(metadataConn, false),
                connectorConfig.maxRetries(), connectorConfig.retryDelay());
    }

    private SlotCreateOrExportResult exportSnapshotFromExistingSlot(SlotState slotInfo) {
        try {
            final PostgresConnection holder = connectionFactory.newConnection();
            register(holder, () -> this.snapshotHolderConnection = holder);
            holder.connection().setAutoCommit(false);
            holder.executeWithoutCommitting(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            String walBefore = holder.queryAndMap(
                    "SELECT pg_current_wal_lsn()::text",
                    holder.singleResultMapper(
                            rs -> rs.getString(1), "Smart snapshot: [role=leader epoch=" + epoch + "] Failed to get WAL LSN"));

            String currentSnapshotName = holder.queryAndMap(
                    "SELECT pg_export_snapshot()",
                    holder.singleResultMapper(
                            rs -> rs.getString(1), "Smart snapshot: [role=leader epoch=" + epoch + "] Failed to export snapshot"));

            // Mirror PostgresSnapshotChangeEventSource#getTransactionStartLsn: only resume from the slot's
            // last flushed LSN when the snapshotter does NOT stream starting from the snapshot point (e.g.
            // recovery modes). For modes that stream from the snapshot (initial, when_needed), stream from the
            // snapshot's consistent point; the slot's confirmed_flush_lsn can be behind it and would otherwise
            // resume streaming from before the snapshot.
            String currentSlotLsn;
            if (slotInfo != null && slotInfo.slotLastFlushedLsn() != null
                    && !snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot()) {
                currentSlotLsn = slotInfo.slotLastFlushedLsn().asString();
            }
            else {
                currentSlotLsn = walBefore;
            }
            LOGGER.info("Smart snapshot: [role=leader epoch={}] Exported snapshot={}, LSN={}",
                    epoch, currentSnapshotName, currentSlotLsn);

            return new SlotCreateOrExportResult(null, slotInfo, currentSnapshotName, currentSlotLsn);
        }
        catch (SQLException e) {
            releaseSnapshot();
            throw new DebeziumException("Smart snapshot: [role=leader epoch=" + epoch + "] Failed to export snapshot", e);
        }
    }

    private SlotCreateOrExportResult exportSnapshotWithoutSlot() {
        try {
            final PostgresConnection holder = connectionFactory.newConnection();
            register(holder, () -> this.snapshotHolderConnection = holder);
            holder.connection().setAutoCommit(false);
            holder.executeWithoutCommitting(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            // No slot, so no slot LSN. Use current WAL position as reference.
            String currentSlotLsn = holder.queryAndMap(
                    "SELECT pg_current_wal_lsn()::text",
                    holder.singleResultMapper(
                            rs -> rs.getString(1), "Smart snapshot: [role=leader epoch=" + epoch + "] Failed to get WAL LSN"));

            String currentSnapshotName = holder.queryAndMap(
                    "SELECT pg_export_snapshot()",
                    holder.singleResultMapper(
                            rs -> rs.getString(1), "Smart snapshot: [role=leader epoch=" + epoch + "] Failed to export snapshot"));

            LOGGER.info("Smart snapshot: [role=leader epoch={}] Exported snapshot={}, LSN={} (no-stream)",
                    epoch, currentSnapshotName, currentSlotLsn);

            return new SlotCreateOrExportResult(null, null, currentSnapshotName, currentSlotLsn);
        }
        catch (SQLException e) {
            releaseSnapshot();
            throw new DebeziumException("Smart snapshot: [role=leader epoch=" + epoch + "] Failed to export snapshot", e);
        }
    }

    @Override
    public void keepAlive() {
        final PostgresConnection snapshotHolderConn;
        final PostgresConnection schemaSnapshotConn;
        final ReplicationConnection replicationConn;
        stateLock.lock();
        try {
            snapshotHolderConn = this.snapshotHolderConnection;
            schemaSnapshotConn = this.schemaSnapshotConnection;
            replicationConn = this.replicationConnection;
        }
        finally {
            stateLock.unlock();
        }
        pingOrThrow(snapshotHolderConn, "snapshot-holder");
        pingOrThrow(schemaSnapshotConn, "schema-snapshot-holder");
        if (replicationConn != null) {
            try {
                if (!replicationConn.isConnected()) {
                    throw new DebeziumException("Smart snapshot: [role=leader epoch=" + epoch + "] Replication connection is no longer connected");
                }
            }
            catch (SQLException e) {
                throw new DebeziumException("Smart snapshot: [role=leader epoch=" + epoch + "] Replication connection liveness check failed", e);
            }
        }
    }

    private void pingOrThrow(PostgresConnection conn, String identifier) {
        if (conn == null) {
            return;
        }
        try {
            // runs inside the open snapshot/lock transaction — harmless, resets idle timer
            conn.executeWithoutCommitting("SELECT 1");
        }
        catch (SQLException e) {
            throw new DebeziumException("Smart snapshot: [role=leader epoch=" + epoch + "] " + identifier + " connection is dead (held snapshot lost)", e);
        }
    }

    @Override
    public void releaseSnapshot() {
        final ReplicationConnection replicationConn;
        final PostgresConnection snapshotHolderConn;
        final PostgresConnection schemaSnapshotConn;
        final PostgresConnection replicationMetadataConn;
        stateLock.lock();
        try {
            released = true;
            replicationConn = this.replicationConnection;
            this.replicationConnection = null;
            snapshotHolderConn = this.snapshotHolderConnection;
            this.snapshotHolderConnection = null;
            schemaSnapshotConn = this.schemaSnapshotConnection;
            this.schemaSnapshotConnection = null;
            replicationMetadataConn = this.replicationMetadataConnection;
            this.replicationMetadataConnection = null;
        }
        finally {
            stateLock.unlock();
        }
        // Close the connections without holding the lock. For a connection that is busy with a
        // query, close() waits up to WAIT_FOR_CLOSE_SECONDS and then aborts it. This stops a
        // prepareSnapshot() that is waiting on a query on the leader prep thread.
        closeQuietly(replicationConn, "replication", epoch);
        closeQuietly(snapshotHolderConn, "snapshot holder", epoch);
        closeQuietly(schemaSnapshotConn, "schema snapshot holder", epoch);
        closeQuietly(replicationMetadataConn, "replication metadata", epoch);
    }

    /**
     * Records a newly opened connection in the guarded state. If releaseSnapshot() has already
     * run, the connection is closed at once and this method throws. This makes prepareSnapshot
     * stop rather than keep a connection that will never be released, or publish a snapshot
     * during shutdown.
     */
    private void register(AutoCloseable resource, Runnable connectionAssignment) {
        final boolean alreadyReleased;
        stateLock.lock();
        try {
            alreadyReleased = released;
            if (!alreadyReleased) {
                connectionAssignment.run();
            }
        }
        finally {
            stateLock.unlock();
        }
        if (alreadyReleased) {
            closeQuietly(resource, "opened after release", epoch);
            throw new DebeziumException("Smart snapshot: [role=leader epoch=" + epoch + "] Snapshot released during preparation; aborting");
        }
    }

    private static void closeQuietly(AutoCloseable resource, String identifier, Integer epoch) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        }
        catch (Exception e) {
            LOGGER.warn("Smart snapshot: [role=leader epoch={}] Error closing {} connection", epoch, identifier, e);
        }
    }

    static class PostgresLeaderSchemaSource extends PostgresSnapshotChangeEventSource {
        private final PostgresConnection connection;
        // the leader's pg_export_snapshot / new-slot exported name
        private final String exportedSnapshotName;
        private final Integer epoch;

        PostgresLeaderSchemaSource(PostgresConnectorConfig connectorConfig, SnapshotterService snapshotterService,
                                   MainConnectionProvidingConnectionFactory<PostgresConnection> heldConnectionFactory,
                                   PostgresSchema schema, EventDispatcher<PostgresPartition, TableId> dispatcher, Clock clock,
                                   NotificationService<PostgresPartition, PostgresOffsetContext> notificationService,
                                   SlotCreationResult slotCreatedInfo, SlotState startingSlotInfo,
                                   // exported name -> import it EXPLICITLY (works for new AND existing slot)
                                   String exportedSnapshotName, Integer epoch) {
            super(connectorConfig, snapshotterService, heldConnectionFactory, schema, dispatcher, clock, SnapshotProgressListener.NO_OP(),
                    slotCreatedInfo, startingSlotInfo, notificationService);
            this.connection = heldConnectionFactory.mainConnection();
            this.exportedSnapshotName = exportedSnapshotName;
            this.epoch = epoch;
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
         * The normal single-task snapshot code only issues SET TRANSACTION SNAPSHOT in case 1. In case 2 it
         * starts a plain transaction with no SET TRANSACTION SNAPSHOT, so the session reads at its OWN,later
         * point-in-time, okay for a single reader, but wrong for the leader and multiple followers, which must match the tasks.
         * So we always issue it here:  SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;  SET TRANSACTION SNAPSHOT '<name>';
         */
        @Override
        protected void setSnapshotTransactionIsolationLevel(boolean isOnDemand) throws SQLException {
            if (exportedSnapshotName != null && !isOnDemand) {
                LOGGER.info("Smart snapshot: [role=leader epoch={}] Setting transaction isolation level on exported snapshot={}", epoch, exportedSnapshotName);
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
            LOGGER.info("Smart snapshot: [role=leader epoch={}] Determining captured tables", epoch);
            // discover UNDER the snapshot
            determineCapturedTables(ctx, getDataCollectionPattern(task.getDataCollections()), task);
            LOGGER.info("Smart snapshot: [role=leader epoch={}] Optionally locking tables for schema snapshot", epoch);
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

        public SlotCreateOrExportResult(
                                        SlotCreationResult slotCreationResult,
                                        SlotState startingSlotState,
                                        String currentSnapshotName,
                                        String currentSlotLsn) {
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
