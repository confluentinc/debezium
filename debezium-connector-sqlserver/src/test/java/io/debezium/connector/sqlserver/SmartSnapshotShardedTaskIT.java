/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.pipeline.source.snapshot.KafkaLogSnapshotCoordination;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * Tier-B IT (per sqlserver-smart-snapshot-test-plan.md): drives a single sharded smart-snapshot task against a
 * real SQL Server, with the coordination topic on a real (test-only) Kafka broker. The embedded engine always
 * runs exactly one task and never invokes {@code Connector.taskConfigs()} with {@code maxTasks > 1} (see the
 * test plan's "Harness reality" section), so this test manually stamps the task config fields that
 * {@code SqlServerSmartSnapshotCoordinators.taskConfigs()} would have produced for shard 0 of a 2-shard round,
 * and manually publishes {@code L_db} to the coordination topic the way {@code SqlServerSmartSnapshotCoordinators
 * .start()} would -- i.e. it validates the *task* half (§ schema-writer full-structure dispatch,
 * shard-only data snapshot, per-shard completion record), not the Connector's fan-out/monitor/downscale
 * (that needs the Tier-C {@code EmbeddedConnectCluster} harness called out as future work in the test plan).
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
    public void shardedTaskSnapshotsOnlyItsShardButWriterDispatchesFullSchema() throws Exception {
        String coordinationTopic = serverName + "." + TestHelper.TEST_DATABASE_1 + ".snapshot-coordination";
        Map<String, Object> clientConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        KafkaLogSnapshotCoordination coordination = new KafkaLogSnapshotCoordination(clientConfig, coordinationTopic, "test-connector");
        coordination.start();
        try {
            // Simulate what SqlServerSmartSnapshotCoordinators.start() does: the Connector captures L_db and
            // publishes it *before* any task exists.
            Lsn lDb = connection.getMaxLsn(TestHelper.TEST_DATABASE_1);
            Map<String, Object> shared = new HashMap<>();
            shared.put(SmartSnapshotConnectorCoordinator.SLOT_LSN_KEY, lDb.toString());
            shared.put(SmartSnapshotConnectorCoordinator.EPOCH_KEY, 1);
            coordination.write(Collect.hashMapOf("server", serverName), shared);

            // Simulate a shard-0/2 task config, as SqlServerSmartSnapshotCoordinators.taskConfigs() would
            // stamp it: index 0 -> the schema-history writer, shard narrowed to table1 only.
            Configuration config = TestHelper.defaultConfig()
                    .with(CommonConnectorConfig.TOPIC_PREFIX, serverName)
                    .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, true)
                    .with(CommonConnectorConfig.SMART_SNAPSHOT_COORDINATION_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS)
                    .with(ConfigurationNames.TASK_ID_PROPERTY_NAME, "0")
                    .with(SqlServerConnectorConfig.SMART_SNAPSHOT_DATABASE_TASK_INDEX_PROPERTY_NAME, "0")
                    .with(SmartSnapshotConnectorCoordinator.EPOCH_KEY, "1")
                    .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "[A-z].*dbo.table1")
                    .build();

            start(SqlServerConnector.class, config);
            assertConnectorIsRunning();

            SourceRecords records = consumeRecordsByTopic(5);
            assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table1")).hasSize(5);
            assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table2")).isNullOrEmpty();

            // Writer (index 0) must dispatch the FULL DB schema, not just its own shard (design §6.2) --
            // otherwise a later collapsed streaming task could never recover table2's structure.
            String schemaHistoryContent = java.nio.file.Files.readString(TestHelper.SCHEMA_HISTORY_PATH);
            assertThat(schemaHistoryContent).contains("table1");
            assertThat(schemaHistoryContent).contains("table2");

            // Completion record written under the per-DB-local coordination index (not the global task.id --
            // here they're both "0" so this alone doesn't disambiguate them, but it confirms the write shape).
            // writeCompleted() runs just after the last row is dispatched, so it can still be in flight when
            // consumeRecordsByTopic() above returns -- poll with readSync() (forces this consumer to catch up
            // to the task's write) until it shows up, rather than a single racy read.
            Map<String, Object>[] completedHolder = new Map[1];
            Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                completedHolder[0] = coordination.readSync(SmartSnapshotConnectorCoordinator.completedKey(serverName, "0"));
                assertThat(completedHolder[0]).isNotNull();
            });
            Map<String, Object> completed = completedHolder[0];
            assertThat(completed.get(SmartSnapshotConnectorCoordinator.COMPLETED_KEY)).isEqualTo(true);
            assertThat(completed.get(SmartSnapshotConnectorCoordinator.EPOCH_KEY)).isEqualTo(1);
        }
        finally {
            coordination.stop();
        }
    }
}
