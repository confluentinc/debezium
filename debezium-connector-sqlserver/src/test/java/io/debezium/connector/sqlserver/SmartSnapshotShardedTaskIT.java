/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Testing;

/**
 * Real Connector-driven multi-task smart-snapshot fan-out against a real SQL Server, with the coordination
 * topic on a real (test-only) Kafka broker. Table-to-shard assignment is now computed independently by each
 * task from the shared {@code snapshot_info} record (design's {@code SnapshotCoordinationFacade.tablesForTask}
 * -- deterministic round-robin by table name), so this test lets the real {@code SqlServerConnector.start()}
 * discover tables/capture {@code L_db} and the real {@code AsyncEmbeddedEngine} start one real task per
 * {@code taskConfigs()} entry (it reads {@code tasks.max} from config and is not hardcoded to a single task,
 * unlike the older blocking embedded engine) -- no hand-stamped task config needed.
 *
 * <p>Requires a real Kafka broker reachable at {@code smart.snapshot.coordination.bootstrap.servers}
 * (override via the {@code test.smart.snapshot.coordination.bootstrap.servers} system property; defaults to
 * {@code 127.0.0.1:9092}).
 */
public class SmartSnapshotShardedTaskIT extends AbstractAsyncEngineConnectorTest {

    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getProperty(
            "test.smart.snapshot.coordination.bootstrap.servers", "127.0.0.1:9092");

    private SqlServerConnection connection;
    // unique per test run so a leftover coordination topic/completion record from a prior run (Kafka
    // topics persist across test executions against a long-lived broker) can never be mistaken for
    // this run's state
    private String serverName;

    @Before
    public void before() throws SQLException {
        serverName = "server" + UUID.randomUUID().toString().replace("-", "");
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE table1 (id int, name varchar(30), primary key(id))",
                "CREATE TABLE table2 (id int, name varchar(30), primary key(id))");

        for (int i = 0; i < 5; i++) {
            connection.execute(String.format("INSERT INTO table1 VALUES(%s, '%s')", i, "name" + i));
            connection.execute(String.format("INSERT INTO table2 VALUES(%s, '%s')", i, "name" + i));
        }

        TestHelper.enableTableCdc(connection, "table1");
        TestHelper.enableTableCdc(connection, "table2");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void twoShardFanOutDispatchesEachTasksOwnShard() throws Exception {
        // tasks.max=2 + tables.per.task=1 over 2 tables -> 2 real shard tasks, each with its own
        // Connector-assigned task.id (0 and 1), each independently computing its own shard via
        // SnapshotCoordinationFacade.tablesForTask(allTables, taskId, numTasks).
        Configuration config = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.TOPIC_PREFIX, serverName)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, true)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_COORDINATION_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_TABLES_PER_TASK, 1)
                .with("tasks.max", 2)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(10);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table1")).hasSize(5);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table2")).hasSize(5);

        // Each shard dispatches only its own table (design §6.2 shard-narrowed dispatch, achieved via
        // determineCapturedTables) -- the union across both real shards is what makes schema history
        // complete, not either shard dispatching everything.
        String schemaHistoryContent = java.nio.file.Files.readString(TestHelper.SCHEMA_HISTORY_PATH);
        assertThat(schemaHistoryContent).contains("table1");
        assertThat(schemaHistoryContent).contains("table2");

        SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        SnapshotCoordinationFacade facade = new SnapshotCoordinationFacade(config, connectorConfig);
        try {
            facade.start();
            Integer epoch = facade.readEpoch();
            assertThat(epoch).isEqualTo(1);
            Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(facade.isDone("0", epoch)).isTrue();
                assertThat(facade.isDone("1", epoch)).isTrue();
            });
        }
        finally {
            facade.stop();
        }

        stopConnector();
    }

    @Test
    public void writerAlsoDispatchesEligibleButUncapturedTables() throws Exception {
        // table3 is excluded from data capture (table.exclude.list) but is still eligible for schema tracking
        // under the default store.only.captured.tables.ddl=false -- the schema-history writer (task.id==0)
        // must additionally dispatch it (design §6.2 leftover set), or smart snapshot would silently
        // under-populate schema-history relative to single-task mode.
        connection.execute("CREATE TABLE table3 (id int, name varchar(30), primary key(id))");

        Configuration config = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.TOPIC_PREFIX, serverName)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, true)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_COORDINATION_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_TABLES_PER_TASK, 1)
                .with(RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST, "testDB1.dbo.table3")
                .with("tasks.max", 2)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(10);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table1")).hasSize(5);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table2")).hasSize(5);

        String schemaHistoryContent = java.nio.file.Files.readString(TestHelper.SCHEMA_HISTORY_PATH);
        assertThat(schemaHistoryContent).contains("table1");
        assertThat(schemaHistoryContent).contains("table2");
        assertThat(schemaHistoryContent).contains("table3");

        stopConnector();
    }

    @Test
    public void taskRestartNeededRepublishesSnapshotInfoAtBumpedEpoch() throws Exception {
        // Gap 2: SqlServerSmartSnapshotChangeEventSourceCoordinator#isLDbStale signals restart_needed when
        // L_db has aged past CDC retention; the shared SmartSnapshotConnectorCoordinator bumps the epoch and
        // hands out new task configs for it, but never republishes snapshot_info itself. Without
        // SqlServerConnector#republishIfEpochAdvanced, the new epoch's shard tasks would poll a snapshot_info
        // still tagged with the OLD epoch forever and time out. Pre-seeding restart_needed for epoch 1/task 0
        // *before* the round even starts (rather than trying to race a real CDC-retention trigger or a real
        // shard task's own completion timing) makes the very first monitor tick force the restart
        // deterministically -- the monitor always checks restart_needed before completion, every tick, so it
        // fires regardless of how fast the tiny epoch-1 shards would otherwise finish.
        Configuration config = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.TOPIC_PREFIX, serverName)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, true)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_COORDINATION_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_TABLES_PER_TASK, 1)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_MONITOR_POLL_INTERVAL_MS, 1000)
                .with("tasks.max", 2)
                .build();

        SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        SnapshotCoordinationFacade seed = new SnapshotCoordinationFacade(config, connectorConfig);
        seed.start();
        seed.writeRestartNeeded("0", 1);
        seed.stop();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SnapshotCoordinationFacade facade = new SnapshotCoordinationFacade(config, connectorConfig);
        try {
            facade.start();
            Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(facade.readEpoch()).isEqualTo(2);
                Map<String, Object> snapshotInfo = facade.readSnapshotInfo();
                assertThat(SnapshotCoordinationFacade.epochOf(snapshotInfo)).isEqualTo(2);
                assertThat(snapshotInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT)).isNotNull();
            });

            Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(facade.isDone("0", 2)).isTrue();
                assertThat(facade.isDone("1", 2)).isTrue();
            });
        }
        finally {
            facade.stop();
        }

        stopConnector();
    }
}
