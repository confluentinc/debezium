/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager.SnapshotSetup;
import io.debezium.relational.TableId;

public class SqlServerSnapshotLifecycleManagerTest {

    // fast enough for a unit test but exercises the same "poll a few times, then give up" logic
    private static final Duration TEST_MAX_WAIT = Duration.ofMillis(50);
    private static final Duration TEST_POLL_INTERVAL = Duration.ofMillis(10);

    private SqlServerConnectorConfig minimalConfig() {
        return new SqlServerConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                        // Kafka Connect always injects this into the raw worker config; needed here because
                        // prepareSnapshot() now stands up a real SnapshotterService (design §15.7), whose
                        // provider chain resolves connector-specific implementations via this property.
                        .with("connector.class", SqlServerConnector.class.getName())
                        .build());
    }

    /**
     * A minimal dynamic-proxy {@link Connection} answering only what
     * {@code SqlServerSnapshotChangeEventSource.connectionCreated()} needs
     * (@code getTransactionIsolation()}) -- avoids a real network connect attempt in
     * {@link StubConnection#connection()}. Every other method returns a JDK-default value (0/false/null),
     * which is fine since nothing else in the discovery-only call chain touches the connection.
     */
    private static Connection fakeJdbcConnection() {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("getTransactionIsolation".equals(method.getName())) {
                return Connection.TRANSACTION_READ_COMMITTED;
            }
            Class<?> returnType = method.getReturnType();
            if (returnType == boolean.class) {
                return false;
            }
            if (returnType.isPrimitive() && returnType != void.class) {
                return 0;
            }
            return null;
        };
        return (Connection) Proxy.newProxyInstance(SqlServerSnapshotLifecycleManagerTest.class.getClassLoader(),
                new Class<?>[]{ Connection.class }, handler);
    }

    /** A connection stub that never touches the network -- overrides getMaxLsn()/readTableNames(), no-op close(). */
    private static class StubConnection extends SqlServerConnection {
        private final List<Lsn> answers;
        private final Set<TableId> tables;
        private int callCount;

        StubConnection(SqlServerConnectorConfig config, List<Lsn> answers, Set<TableId> tables) {
            super(config, null, Collections.emptySet(), config.useSingleDatabase());
            this.answers = answers;
            this.tables = tables;
        }

        @Override
        public Lsn getMaxLsn(String databaseName) {
            Lsn answer = answers.get(Math.min(callCount, answers.size() - 1));
            callCount++;
            return answer;
        }

        @Override
        public Set<TableId> readTableNames(String databaseName, String schemaNamePattern, String tableNamePattern, String[] tableTypes) {
            return tables;
        }

        @Override
        public synchronized Connection connection() {
            return fakeJdbcConnection();
        }

        @Override
        public synchronized void close() {
            // no real connection was ever opened
        }
    }

    private static Set<TableId> oneTable() {
        Set<TableId> tables = new LinkedHashSet<>();
        tables.add(new TableId("db1", "dbo", "t1"));
        return tables;
    }

    private static Set<TableId> twoTables() {
        Set<TableId> tables = new LinkedHashSet<>();
        tables.add(new TableId("db1", "dbo", "t1"));
        tables.add(new TableId("db1", "dbo", "t2"));
        return tables;
    }

    // Design §15.7: prepareSnapshot() must call the REAL discovery pipeline (determineCapturedTables()),
    // not a hand-rolled filter -- these two tests guard the two things a hand-rolled filter previously
    // missed: snapshot.include.collection.list restriction, and forced signal-table inclusion.

    @Test
    public void prepareSnapshotRespectsSnapshotIncludeCollectionList() {
        SqlServerConnectorConfig config = new SqlServerConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                        .with("connector.class", SqlServerConnector.class.getName())
                        .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "db1\\.dbo\\.t1")
                        .build());
        Lsn lsn = Lsn.valueOf(new byte[]{ 0x01 });
        StubConnection connection = new StubConnection(config, List.of(lsn), twoTables());

        SqlServerSnapshotLifecycleManager manager = new SqlServerSnapshotLifecycleManager(config, "db1", () -> connection);
        SnapshotSetup setup = manager.prepareSnapshot(true);

        assertThat(setup.tables()).containsExactly(new TableId("db1", "dbo", "t1"));
    }

    @Test
    public void prepareSnapshotAppliesTableIncludeListAndSnapshotIncludeCollectionListTogether() {
        // Fidelity check: a hand-rolled filter could plausibly apply only one of these two independent
        // narrowing filters. The real pipeline ANDs them: table.include.list passes {t1, t2}, then
        // snapshot.include.collection.list narrows that further down to just t1.
        SqlServerConnectorConfig config = new SqlServerConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                        .with("connector.class", SqlServerConnector.class.getName())
                        // SQL Server's table.include.list is schema.table (2-part), not database.schema.table --
                        // the connector's tableIdMapper is `x -> x.schema() + "." + x.table()` (SqlServerConnectorConfig).
                        .with(io.debezium.relational.RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.t1,dbo\\.t2")
                        .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "db1\\.dbo\\.t1")
                        .build());
        Lsn lsn = Lsn.valueOf(new byte[]{ 0x01 });
        StubConnection connection = new StubConnection(config, List.of(lsn), twoTables());

        SqlServerSnapshotLifecycleManager manager = new SqlServerSnapshotLifecycleManager(config, "db1", () -> connection);
        SnapshotSetup setup = manager.prepareSnapshot(true);

        assertThat(setup.tables()).containsExactly(new TableId("db1", "dbo", "t1"));
    }

    @Test
    public void prepareSnapshotReturnsCapturedLsnAndDiscoveredTablesWithNoSnapshotName() {
        SqlServerConnectorConfig config = minimalConfig();
        Lsn lsn = Lsn.valueOf(new byte[]{ 0x01, 0x02 });
        StubConnection connection = new StubConnection(config, List.of(lsn), oneTable());

        SqlServerSnapshotLifecycleManager manager = new SqlServerSnapshotLifecycleManager(config, "db1", () -> connection);
        SnapshotSetup setup = manager.prepareSnapshot(true);

        assertThat(setup.snapshotName()).isNull();
        assertThat(setup.consistentPosition()).isEqualTo(lsn.toString());
        assertThat(setup.tables()).containsExactly(new TableId("db1", "dbo", "t1"));
    }

    @Test
    public void prepareSnapshotReturnsEmptySetupWithoutCapturingLsnWhenNoTablesCaptured() {
        SqlServerConnectorConfig config = minimalConfig();
        StubConnection connection = new StubConnection(config, List.of(Lsn.NULL), Collections.emptySet());

        SqlServerSnapshotLifecycleManager manager = new SqlServerSnapshotLifecycleManager(config, "db1", () -> connection);
        SnapshotSetup setup = manager.prepareSnapshot(true);

        assertThat(setup.tables()).isEmpty();
        assertThat(setup.consistentPosition()).isNull();
    }

    @Test
    public void prepareSnapshotRetriesUntilLsnBecomesAvailable() {
        SqlServerConnectorConfig config = minimalConfig();
        Lsn lsn = Lsn.valueOf(new byte[]{ 0x0A });
        // first two polls: CDC capture job hasn't populated yet (NULL); third: available
        StubConnection connection = new StubConnection(config, List.of(Lsn.NULL, Lsn.NULL, lsn), oneTable());

        SqlServerSnapshotLifecycleManager manager = new SqlServerSnapshotLifecycleManager(
                config, "db1", () -> connection, TEST_MAX_WAIT, TEST_POLL_INTERVAL);
        SnapshotSetup setup = manager.prepareSnapshot(true);

        assertThat(setup.consistentPosition()).isEqualTo(lsn.toString());
    }

    @Test
    public void prepareSnapshotFailsAfterTimeoutWhenLsnNeverBecomesAvailable() {
        SqlServerConnectorConfig config = minimalConfig();
        StubConnection connection = new StubConnection(config, List.of(Lsn.NULL), oneTable());

        SqlServerSnapshotLifecycleManager manager = new SqlServerSnapshotLifecycleManager(
                config, "db1", () -> connection, TEST_MAX_WAIT, TEST_POLL_INTERVAL);

        assertThatThrownBy(() -> manager.prepareSnapshot(true))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("db1");
    }

    @Test
    public void onAllTasksStartedTransactionReleaseSnapshotAndKeepAliveAreNoOps() {
        SqlServerConnectorConfig config = minimalConfig();
        SqlServerSnapshotLifecycleManager manager = new SqlServerSnapshotLifecycleManager(config, "db1", () -> {
            throw new AssertionError("connection supplier should not be invoked");
        });

        manager.onAllTasksStartedTransaction();
        manager.releaseSnapshot();
        manager.keepAlive();
        // no exceptions, nothing held
    }
}
