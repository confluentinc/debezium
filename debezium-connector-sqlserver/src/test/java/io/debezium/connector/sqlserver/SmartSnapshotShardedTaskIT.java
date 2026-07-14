/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
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
 * Real Connector-driven multi-task smart-snapshot fan-out against a real SQL Server + Kafka broker.
 * {@code SqlServerConnector.start()} stands up the coordinator; task-0's leader thread discovers tables,
 * captures {@code L_db}, and publishes {@code snapshot_info}; each task computes its own shard via
 * {@link SnapshotCoordinationFacade#tablesForTask} and snapshots it, with task-0 owning the full schema
 * history behind a barrier the other shards wait on.
 *
 * <p>Requires a real Kafka broker (default {@code 127.0.0.1:9092}, override via
 * {@code test.smart.snapshot.coordination.bootstrap.servers}).
 */
public class SmartSnapshotShardedTaskIT extends AbstractAsyncEngineConnectorTest {

    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getProperty(
            "test.smart.snapshot.coordination.bootstrap.servers", "127.0.0.1:9092");

    private SqlServerConnection connection;
    // unique per run so a leftover coordination topic/completion record from a prior run can't be mistaken
    // for this run's state (Kafka topics persist across executions against a long-lived broker).
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
        // tasks.max=2 over 2 tables -> 2 real shard tasks (task.id 0 and 1), each computing its own shard via
        // tablesForTask. task-0 is the leader (publishes snapshot_info) and the schema-history writer.
        Configuration config = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.TOPIC_PREFIX, serverName)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, true)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_COORDINATION_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS)
                .with("tasks.max", 2)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(10);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table1")).hasSize(5);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table2")).hasSize(5);

        // task-0 owns the full schema history: both tables' CREATE TABLE come from it.
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
                assertThat(facade.isTaskDone("0", epoch)).isTrue();
                assertThat(facade.isTaskDone("1", epoch)).isTrue();
            });
        }
        finally {
            facade.stop();
        }

        stopConnector();
    }

    @Test
    public void task0WritesFullSchemaHistoryIncludingUncapturedEligibleTables() throws Exception {
        // table3 is excluded from data capture (table.exclude.list) but is still schema-eligible under the
        // default store.only.captured.tables.ddl=false. task-0 owns the FULL schema history, so table3's
        // CREATE TABLE must appear even though no task snapshots its data.
        connection.execute("CREATE TABLE table3 (id int, name varchar(30), primary key(id))");

        Configuration config = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.TOPIC_PREFIX, serverName)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, true)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_COORDINATION_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS)
                // SQL Server's table.exclude.list is schema.table (2-part), not database.schema.table.
                .with(RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST, "dbo\\.table3")
                .with("tasks.max", 2)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(10);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table1")).hasSize(5);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table2")).hasSize(5);
        // Proves the exclusion took effect: table3 has schema but zero data (a silently-broken exclude would
        // capture it as a shard and this would fail).
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table3")).isNull();

        String schemaHistoryContent = java.nio.file.Files.readString(TestHelper.SCHEMA_HISTORY_PATH);
        assertThat(schemaHistoryContent).contains("table1");
        assertThat(schemaHistoryContent).contains("table2");
        assertThat(schemaHistoryContent).contains("table3");

        stopConnector();
    }

    @Test
    public void taskRestartNeededForcesFullRestartAtBumpedEpoch() throws Exception {
        // Pre-seed restart_needed for epoch 1/task 0 before the round starts: the monitor checks it every tick
        // (before completion), so the first tick after the coordinator starts forces a full restart -> epoch
        // bumps to 2, the leader re-publishes at epoch 2, and both shards complete at epoch 2. This is the
        // "one task failure -> full restart" mechanism, driven deterministically.
        Configuration config = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.TOPIC_PREFIX, serverName)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, true)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_COORDINATION_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS)
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
            Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> assertThat(facade.readEpoch()).isEqualTo(2));
            Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(facade.isTaskDone("0", 2)).isTrue();
                assertThat(facade.isTaskDone("1", 2)).isTrue();
            });
        }
        finally {
            facade.stop();
        }

        stopConnector();
    }
}
