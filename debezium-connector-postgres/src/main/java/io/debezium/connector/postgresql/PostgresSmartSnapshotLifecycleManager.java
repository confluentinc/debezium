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
import io.debezium.pipeline.source.snapshot.SmartSnapshotHeldConnectionRegistry;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

public class PostgresSmartSnapshotLifecycleManager implements SmartSnapshotLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSmartSnapshotLifecycleManager.class);

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

    // All the connections the leader keeps open until every task has imported the snapshot, registered here as
    // they are opened. Depending on the path these are:
    // - the new-slot replication connection: runs CREATE_REPLICATION_SLOT (dropSlotOnClose=false, so the slot
    // persists for streaming) and holds the slot's exported snapshot alive. Registered close-only; it has its
    // own liveness check in keepAlive() via the replicationConnection field below (it cannot run SELECT 1).
    // - the replication metadata connection: the JDBC companion the replication builder needs (type registry /
    // schema metadata). No snapshot role of its own — registered only so it is closed alongside the others.
    // - the snapshot-holder connection (existing-slot / no-slot paths): runs pg_export_snapshot() inside a
    // REPEATABLE READ txn to produce the shared snapshot; must stay open, since the exported snapshot is only
    // valid while this txn lives.
    // - the schema-snapshot connection: imports the shared snapshot (SET TRANSACTION SNAPSHOT), discovers the
    // tables under it, and holds ACCESS SHARE (if configured via snapshot.locking.mode) on them.
    // Thread-safe: releaseSnapshot() (task-stop thread) can close these while prepareSnapshot() (leader prep
    // thread) is blocked in a non-interruptible JDBC call, aborting it.
    private final SmartSnapshotHeldConnectionRegistry heldConnections;

    // The new-slot replication connection (dropSlotOnClose=false, so the slot persists for streaming). Kept as a
    // field only for its liveness check in keepAlive(); it is registered with heldConnections for closing.
    private volatile ReplicationConnection replicationConnection;

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
        this.heldConnections = new SmartSnapshotHeldConnectionRegistry("Smart snapshot: [role=leader epoch=" + epoch + "]");

        // needed specifically to build inner class PostgresSmartSnapshotLeaderSchemaSource
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.notificationService = notificationService;
        this.clock = clock;
    }

    /**
     * prepareSnapshot, keepAlive and releaseSnapshot share the held connections and may run on two threads at
     * once: the leader thread and the task-stop thread. The concurrency is handled by
     * {@link SmartSnapshotHeldConnectionRegistry}: prepareSnapshot runs its database work without a lock and registers each
     * connection as it opens it, while releaseSnapshot closes them (aborting any prepare still in progress).
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
            // Work through the local 'holder' reference. heldConnections tracks it so releaseSnapshot() can
            // close it; if a release closes it underneath us, the next call on the local fails with a normal
            // "connection closed" SQLException, which is caught and treated as a shutdown.
            final PostgresConnection holder = connectionFactory.newConnection();
            heldConnections.registerConnection("schema snapshot", holder);
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
        // registered as a resource (closed on release) but not liveness-checked via SELECT 1 — it has its own
        // isConnected() check in keepAlive(), so it is also kept as a field.
        heldConnections.registerResource("replication", replConn);
        this.replicationConnection = replConn;

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
        // JDBC companion for the replication builder; no snapshot role, close-only.
        heldConnections.registerResource("replication metadata", metadataConn);
        // dropSlotOnClose=false so the slot persists for streaming after downscale
        return PostgresConnectorTask.createReplicationConnectionWithRetry(
                () -> taskContext.createReplicationConnection(metadataConn, false),
                connectorConfig.maxRetries(), connectorConfig.retryDelay());
    }

    private SlotCreateOrExportResult exportSnapshotFromExistingSlot(SlotState slotInfo) {
        try {
            final PostgresConnection holder = connectionFactory.newConnection();
            heldConnections.registerConnection("snapshot holder", holder);
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
            heldConnections.registerConnection("snapshot holder", holder);
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
        // Pings the snapshot-holder / schema-snapshot connections via SELECT 1.
        heldConnections.keepAlive();
        // The replication connection can't run SELECT 1, so it has its own liveness check.
        final ReplicationConnection replicationConn = this.replicationConnection;
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

    @Override
    public void releaseSnapshot() {
        // Closing the held connections without holding a lock. For a connection busy with a query, close() waits
        // then aborts it, which stops a prepareSnapshot() waiting on a query on the leader prep thread.
        heldConnections.close();
        this.replicationConnection = null;
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
         * The base {@link PostgresSnapshotChangeEventSource#connectionCreated} only opens the snapshot
         * transaction when {@code shouldStreamEventsStartingFromSnapshot() && startingSlotInfo == null}.
         * For the existing-slot path we pass a non-null startingSlotInfo, so the base gate would skip the
         * import entirely and the leader would discover/lock tables at its own point-in-time instead of the
         * exported snapshot. The leader always has an exported snapshot to import, so open the transaction
         * unconditionally here.
         */
        @Override
        protected void connectionCreated(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
                throws Exception {
            setSnapshotTransactionIsolationLevel(snapshotContext.onDemand);
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
