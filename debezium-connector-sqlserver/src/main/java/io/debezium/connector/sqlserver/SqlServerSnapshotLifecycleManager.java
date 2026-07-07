/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Smart-snapshot anchor capture for the SQL Server {@code repeatable_read} design: discovers the database's
 * captured tables and captures {@code L_db = fn_cdc_get_max_lsn()} once, with nothing held afterward -- no
 * barrier, no lock-holder connection, no exported snapshot. Called directly and synchronously from
 * Connector-side code ({@link SqlServerConnector#start}) rather than from a task-owned leader thread: a
 * one-shot query with nothing to keep alive has none of the long-lived-connection/DND concerns that
 * motivated Postgres's move to a task-0 leader thread, so the simpler Connector-managed shape is kept.
 */
public class SqlServerSnapshotLifecycleManager implements SmartSnapshotLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSnapshotLifecycleManager.class);

    // CDC capture job may not have populated a max LSN yet (freshly-enabled CDC) -- bounded retry, then fail.
    private static final Duration DEFAULT_MAX_LSN_WAIT = Duration.ofMinutes(5);
    private static final Duration DEFAULT_MAX_LSN_POLL_INTERVAL = Duration.ofSeconds(10);

    private final SqlServerConnectorConfig connectorConfig;
    private final String databaseName;
    private final Supplier<SqlServerConnection> connectionSupplier;
    private final Duration maxLsnWait;
    private final Duration maxLsnPollInterval;
    // Eligible-for-schema tables (RelationalTableFilters#eligibleForSchemaDataCollectionFilter) that aren't
    // in the data-capture set -- only non-empty under store.only.captured.tables.ddl=false (the default).
    // Populated as a side effect of prepareSnapshot(); the schema-history writer (task.id==0) additionally
    // dispatches these so smart snapshot doesn't silently under-populate schema-history relative to
    // single-task mode (design §6.2).
    private volatile List<TableId> uncapturedEligibleTables = Collections.emptyList();

    public SqlServerSnapshotLifecycleManager(SqlServerConnectorConfig connectorConfig, String databaseName,
                                             Supplier<SqlServerConnection> connectionSupplier) {
        this(connectorConfig, databaseName, connectionSupplier, DEFAULT_MAX_LSN_WAIT, DEFAULT_MAX_LSN_POLL_INTERVAL);
    }

    // package-private: lets tests use short-lived retry/timeout windows instead of the production defaults
    SqlServerSnapshotLifecycleManager(SqlServerConnectorConfig connectorConfig, String databaseName,
                                      Supplier<SqlServerConnection> connectionSupplier,
                                      Duration maxLsnWait, Duration maxLsnPollInterval) {
        this.connectorConfig = connectorConfig;
        this.databaseName = databaseName;
        this.connectionSupplier = connectionSupplier;
        this.maxLsnWait = maxLsnWait;
        this.maxLsnPollInterval = maxLsnPollInterval;
    }

    @Override
    public SnapshotSetup prepareSnapshot(boolean shouldStream) {
        try (SqlServerConnection connection = connectionSupplier.get()) {
            Set<TableId> allTables = connection.readTableNames(databaseName, null, null, new String[]{ "TABLE" });
            List<TableId> tables = allTables.stream()
                    .filter(tableId -> connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                    .collect(Collectors.toList());
            this.uncapturedEligibleTables = allTables.stream()
                    .filter(tableId -> !tables.contains(tableId))
                    .filter(tableId -> connectorConfig.getTableFilters().eligibleForSchemaDataCollectionFilter().isIncluded(tableId))
                    .collect(Collectors.toList());
            if (tables.isEmpty()) {
                return new SnapshotSetup(null, null, tables);
            }
            Lsn lsn = awaitMaxLsn(connection);
            LOGGER.info("Smart snapshot: [{}] captured L_db={} for {} table(s), {} eligible-but-uncaptured for schema-history",
                    databaseName, lsn, tables.size(), uncapturedEligibleTables.size());
            return new SnapshotSetup(null, lsn.toString(), tables);
        }
        catch (SQLException e) {
            throw new DebeziumException("Smart snapshot: [" + databaseName + "] failed to discover tables / capture max LSN", e);
        }
    }

    /**
     * Tables eligible for schema-history tracking but not in the data-capture set (design §6.2 leftover
     * set) -- only meaningful after {@link #prepareSnapshot} has run.
     */
    public List<TableId> getUncapturedEligibleTables() {
        return uncapturedEligibleTables;
    }

    private Lsn awaitMaxLsn(SqlServerConnection connection) throws SQLException {
        Metronome metronome = Metronome.parker(maxLsnPollInterval, Clock.SYSTEM);
        Instant deadline = Instant.now().plus(maxLsnWait);
        while (true) {
            Lsn lsn = connection.getMaxLsn(databaseName);
            if (lsn.isAvailable()) {
                return lsn;
            }
            if (Instant.now().isAfter(deadline)) {
                throw new DebeziumException("Smart snapshot: [" + databaseName + "] CDC capture job has not "
                        + "populated a max LSN within " + maxLsnWait
                        + " -- verify CDC is enabled and the capture job is running for this database");
            }
            LOGGER.info("Smart snapshot: [{}] max LSN not yet available (CDC capture job not yet populated), retrying...", databaseName);
            try {
                metronome.pause();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DebeziumException("Smart snapshot: [" + databaseName + "] interrupted while waiting for max LSN", e);
            }
        }
    }

    @Override
    public void onAllTasksStartedTransaction() {
        // no-op: repeatable_read holds no barrier to release (design §3.3)
    }

    @Override
    public void releaseSnapshot() {
        // no-op: no connections held past prepareSnapshot()
    }

    @Override
    public void keepAlive() {
        // no-op: nothing held
    }
}
