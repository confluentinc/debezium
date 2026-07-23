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
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Smart-snapshot anchor capture, run once per round by the task-0 leader thread: discovers the database's
 * captured tables and captures {@code L_db = fn_cdc_get_max_lsn()}, holding nothing afterward (no barrier,
 * lock-holder connection, or exported snapshot), so the {@code onAllTasksStartedTransaction}/{@code keepAlive}/
 * {@code releaseSnapshot} lifecycle hooks are no-ops.
 */
public class SqlServerSnapshotLifecycleManager implements SmartSnapshotLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSnapshotLifecycleManager.class);

    // CDC capture job may not have populated a max LSN yet (freshly-enabled CDC) -- bounded retry, then fail.
    // Package-private: sibling shards size their snapshot-info wait against this so a slow leader (still in
    // awaitMaxLsn) doesn't time them out. See SqlServerSmartSnapshotChangeEventSourceCoordinator.
    static final Duration DEFAULT_MAX_LSN_WAIT = Duration.ofMinutes(5);
    private static final Duration DEFAULT_MAX_LSN_POLL_INTERVAL = Duration.ofSeconds(10);

    private final SqlServerConnectorConfig connectorConfig;
    private final String databaseName;
    private final Supplier<SqlServerConnection> connectionSupplier;
    private final Duration maxLsnWait;
    private final Duration maxLsnPollInterval;
    // Schema-eligible tables outside the data-capture set (only non-empty under
    // store.only.captured.tables.ddl=false). Populated by prepareSnapshot(); task.id==0 additionally dispatches
    // these so schema history isn't under-populated relative to single-task mode.
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
        SqlServerLeaderSchemaSource.DiscoveryResult discovery;
        // Own connection, closed here via try-with-resources: SqlServerLeaderSchemaSource is a one-off with no
        // lifecycle of its own, so nothing else would release it.
        try (SqlServerConnection discoveryConnection = connectionSupplier.get()) {
            MainConnectionProvidingConnectionFactory<SqlServerConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                    () -> discoveryConnection);
            SnapshotterService snapshotterService = SqlServerLeaderSchemaSource.buildSnapshotterService(connectorConfig);
            SqlServerLeaderSchemaSource leaderSchemaSource = new SqlServerLeaderSchemaSource(connectorConfig, connectionFactory, snapshotterService);
            SqlServerPartition partition = new SqlServerPartition(connectorConfig.getLogicalName(), databaseName);
            discovery = leaderSchemaSource.discover(partition);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new DebeziumException("Smart snapshot: [" + databaseName + "] failed to discover tables", e);
        }

        List<TableId> tables = discovery.capturedTables;
        this.uncapturedEligibleTables = discovery.uncapturedEligibleTables;
        if (tables.isEmpty()) {
            return new SnapshotSetup(null, null, tables);
        }

        try (SqlServerConnection connection = connectionSupplier.get()) {
            Lsn lsn = awaitMaxLsn(connection);
            LOGGER.info("Smart snapshot: [{}] captured L_db={} for {} table(s), {} eligible-but-uncaptured for schema-history",
                    databaseName, lsn, tables.size(), uncapturedEligibleTables.size());
            return new SnapshotSetup(null, lsn.toString(), tables);
        }
        catch (SQLException e) {
            throw new DebeziumException("Smart snapshot: [" + databaseName + "] failed to capture max LSN", e);
        }
    }

    /** Schema-eligible tables outside the data-capture set; only meaningful after {@link #prepareSnapshot}. */
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
        // no-op: repeatable_read holds no barrier to release
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
