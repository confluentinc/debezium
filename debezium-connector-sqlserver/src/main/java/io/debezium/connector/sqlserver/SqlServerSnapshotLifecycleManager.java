/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.pipeline.source.snapshot.SnapshotLifecycleManager;
import io.debezium.relational.TableId;

/**
 * SQL Server implementation of {@link SnapshotLifecycleManager}, scoped to a single database.
 * <p>
 * SQL Server has no exportable snapshot (no {@code pg_export_snapshot()} / {@code SET TRANSACTION SNAPSHOT}),
 * so this follows the <b>MySQL-style lock path</b> rather than the Postgres snapshot-name path:
 * <ul>
 * <li>{@link #prepareSnapshot} holds a {@code TABLOCKX} write barrier on <b>all</b> the database's captured
 * tables (in one open transaction on a dedicated lock-holder connection) and captures the consistent
 * position {@code L_db = fn_cdc_get_max_lsn()}. The barrier freezes writes so every task that opens its own
 * {@code snapshot}-isolation transaction while it is held pins the identical committed state.</li>
 * <li>{@link #snapshotName()} is {@code null} — tasks join by opening their own {@code snapshot}-isolation
 * transaction, not by name.</li>
 * <li>{@link #onAllTasksJoined()} releases the barrier (like MySQL {@code UNLOCK TABLES}).</li>
 * <li>{@link #requiresFullRestartOnTaskFailure()} is {@code true} — once the barrier is released a failed
 * task cannot re-pin {@code L_db}, so the whole database re-coordinates with a new epoch.</li>
 * </ul>
 * One instance exists per database (the Connector creates one {@code SmartSnapshotConnectorCoordinator} per
 * database).
 */
public class SqlServerSnapshotLifecycleManager implements SnapshotLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSnapshotLifecycleManager.class);

    private final SqlServerConnectorConfig connectorConfig;
    private final String databaseName;

    // Holds the TABLOCKX write barrier (open transaction) and keeps the consistent point frozen until all
    // tasks have joined; kept open afterwards purely so isValid() can health-check it.
    private SqlServerConnection lockHolderConnection;
    private volatile boolean barrierReleased;
    private Lsn currentMaxLsn;

    public SqlServerSnapshotLifecycleManager(SqlServerConnectorConfig connectorConfig, String databaseName) {
        this.connectorConfig = connectorConfig;
        this.databaseName = databaseName;
    }

    @Override
    public SnapshotSetup prepareSnapshot(List<TableId> tables, boolean shouldStream) {
        List<TableId> dbTables = tables.stream()
                .filter(t -> databaseName.equals(t.catalog()))
                .collect(Collectors.toList());

        try {
            lockHolderConnection = new SqlServerConnection(connectorConfig, null, Collections.emptySet(), false);
            lockHolderConnection.connection().setAutoCommit(false);
            lockAllTables(dbTables);

            // Capture the consistent position after the write barrier is held so no commit can land between
            // the capture and the tasks pinning their snapshot-isolation view.
            currentMaxLsn = lockHolderConnection.getMaxLsn(databaseName);
            barrierReleased = false;
            LOGGER.info("Smart snapshot [db={}]: locked {} tables, consistent position (max LSN)={}",
                    databaseName, dbTables.size(), currentMaxLsn);
            return new SnapshotSetup(null, currentMaxLsn == null ? null : currentMaxLsn.toString());
        }
        catch (SQLException e) {
            releaseSnapshot();
            throw new DebeziumException("Smart snapshot [db=" + databaseName + "]: failed to prepare snapshot", e);
        }
    }

    private void lockAllTables(List<TableId> tables) throws SQLException {
        LOGGER.info("Smart snapshot [db={}]: acquiring TABLOCKX on {} tables", databaseName, tables.size());
        try (Statement statement = lockHolderConnection.connection().createStatement()) {
            statement.execute("SET LOCK_TIMEOUT " + connectorConfig.snapshotLockTimeout().toMillis());
            for (TableId tableId : tables) {
                String quoted = lockHolderConnection.quotedTableIdString(tableId);
                // TOP(0): acquire the exclusive table lock without scanning rows; held until the transaction
                // is rolled back (onAllTasksJoined) or the connection is closed (releaseSnapshot).
                statement.executeQuery("SELECT TOP(0) * FROM " + quoted + " WITH (TABLOCKX)").close();
            }
        }
    }

    @Override
    public void onAllTasksJoined() {
        // All tasks have opened their snapshot-isolation transactions; the write barrier can be released
        // (their MVCC views are now pinned). Roll back to drop the TABLOCKX locks but keep the connection
        // open so isValid() can still health-check it.
        SqlServerConnection conn = this.lockHolderConnection;
        if (conn != null && !barrierReleased) {
            try {
                conn.connection().rollback();
                barrierReleased = true;
                LOGGER.info("Smart snapshot [db={}]: all tasks joined, released TABLOCKX write barrier", databaseName);
            }
            catch (SQLException e) {
                LOGGER.warn("Smart snapshot [db={}]: error releasing write barrier", databaseName, e);
            }
        }
    }

    @Override
    public boolean requiresFullRestartOnTaskFailure() {
        // No exportable snapshot to re-attach to; once the barrier is released a failed task cannot re-pin
        // the same L_db, so a single task failure requires re-coordinating the whole database (new epoch).
        return true;
    }

    @Override
    public void releaseSnapshot() {
        SqlServerConnection conn = this.lockHolderConnection;
        this.lockHolderConnection = null;
        if (conn != null) {
            try {
                conn.close();
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot [db={}]: error closing lock holder connection", databaseName, e);
            }
        }
        currentMaxLsn = null;
        barrierReleased = false;
    }

    @Override
    public boolean isValid() {
        SqlServerConnection conn = this.lockHolderConnection;
        if (conn == null) {
            return false;
        }
        try {
            // MUST be a non-committing check: the lock holder runs autoCommit(false) and the TABLOCKX write
            // barrier is held by its open transaction. JdbcConnection.execute(...) commits, which would
            // silently drop the barrier; Connection.isValid() does not touch the transaction.
            return conn.connection().isValid(5);
        }
        catch (SQLException e) {
            return false;
        }
    }

    @Override
    public String snapshotName() {
        // SQL Server has no exportable snapshot; tasks open their own snapshot-isolation transaction.
        return null;
    }

    @Override
    public String consistentPosition() {
        return currentMaxLsn == null ? null : currentMaxLsn.toString();
    }
}
