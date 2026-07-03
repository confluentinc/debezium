/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.pipeline.source.snapshot.SnapshotLifecycleManager;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Smart-snapshot anchor capture for the SQL Server {@code repeatable_read} design: captures
 * {@code L_db = fn_cdc_get_max_lsn()} once per database, with nothing held afterward — no barrier, no
 * lock-holder connection, no exported snapshot. Unlike Postgres, this manager is called directly from
 * Connector-side code ({@link SqlServerSmartSnapshotCoordinators}) rather than from a task-owned leader
 * thread: a one-shot query with nothing to keep alive has none of the long-lived-connection/DND concerns
 * that motivated Postgres's move to a task0 leader thread, so the simpler Connector-managed shape is kept.
 */
public class SqlServerSnapshotLifecycleManager implements SnapshotLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSnapshotLifecycleManager.class);

    // CDC capture job may not have populated a max LSN yet (freshly-enabled CDC) -- bounded retry, then fail.
    private static final Duration DEFAULT_MAX_LSN_WAIT = Duration.ofMinutes(5);
    private static final Duration DEFAULT_MAX_LSN_POLL_INTERVAL = Duration.ofSeconds(10);

    private final String databaseName;
    private final Supplier<SqlServerConnection> connectionSupplier;
    private final Duration maxLsnWait;
    private final Duration maxLsnPollInterval;
    private volatile String currentSlotLsn;

    public SqlServerSnapshotLifecycleManager(String databaseName, Supplier<SqlServerConnection> connectionSupplier) {
        this(databaseName, connectionSupplier, DEFAULT_MAX_LSN_WAIT, DEFAULT_MAX_LSN_POLL_INTERVAL);
    }

    // package-private: lets tests use short-lived retry/timeout windows instead of the production defaults
    SqlServerSnapshotLifecycleManager(String databaseName, Supplier<SqlServerConnection> connectionSupplier,
                                      Duration maxLsnWait, Duration maxLsnPollInterval) {
        this.databaseName = databaseName;
        this.connectionSupplier = connectionSupplier;
        this.maxLsnWait = maxLsnWait;
        this.maxLsnPollInterval = maxLsnPollInterval;
    }

    @Override
    public SnapshotSetup prepareSnapshot(List<TableId> tables, boolean shouldStream) {
        try (SqlServerConnection connection = connectionSupplier.get()) {
            Lsn lsn = awaitMaxLsn(connection);
            currentSlotLsn = lsn.toString();
            LOGGER.info("Smart snapshot: [{}] captured L_db={}", databaseName, currentSlotLsn);
            return new SnapshotSetup(null, currentSlotLsn);
        }
        catch (SQLException e) {
            throw new DebeziumException("Smart snapshot: [" + databaseName + "] failed to capture max LSN", e);
        }
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
    public void onAllTasksJoined() {
        // no-op: repeatable_read holds no barrier to release (design §3.3)
    }

    @Override
    public void releaseSnapshot() {
        // no-op: no connections held past prepareSnapshot()
    }

    @Override
    public String snapshotName() {
        // no exportable snapshot under repeatable_read
        return null;
    }

    @Override
    public String consistentPosition() {
        return currentSlotLsn;
    }

    @Override
    public void keepAlive() {
        // no-op: nothing held
    }
}
