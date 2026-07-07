/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                        .build());
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
        public synchronized void close() {
            // no real connection was ever opened
        }
    }

    private static Set<TableId> oneTable() {
        Set<TableId> tables = new LinkedHashSet<>();
        tables.add(new TableId("db1", "dbo", "t1"));
        return tables;
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
