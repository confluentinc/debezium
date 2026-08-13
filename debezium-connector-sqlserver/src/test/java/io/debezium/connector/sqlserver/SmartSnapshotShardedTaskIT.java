/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination.MissingTopicPolicy;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/**
 * Real Connector-driven multi-task smart-snapshot fan-out against a real SQL Server + Kafka broker.
 * {@code SqlServerConnector.start()} stands up the coordinator; task-0's leader thread discovers tables,
 * captures {@code L_db}, and publishes {@code snapshot_info} with a per-task table assignment; each task reads
 * its own pre-assigned shard and snapshots it.
 *
 * <p>task-0 is the sole schema-history writer (it introspects the database and writes CREATE TABLE for the full
 * eligible set under a task-agnostic source); every other shard blocks on task-0's schema-ready marker and then
 * recovers its table structure from the schema-history topic (never introspecting the database), so all shards
 * use the identical schema task-0 captured. This requires a real, shared schema-history store, so these tests
 * use {@link KafkaSchemaHistory} (file-based history caches records per instance and cannot be shared across
 * tasks).
 *
 * <p>Requires a real Kafka broker (default {@code 127.0.0.1:9092}, override via
 * {@code test.smart.snapshot.coordination.bootstrap.servers}); the coordination topic reuses
 * {@code producer.override.bootstrap.servers}, and the schema history uses the same broker.
 */
public class SmartSnapshotShardedTaskIT extends AbstractAsyncEngineConnectorTest {

    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getProperty(
            "test.smart.snapshot.coordination.bootstrap.servers", "127.0.0.1:9092");

    private SqlServerConnection connection;
    // unique per run so a leftover coordination/schema-history topic from a prior run can't be mistaken for this
    // run's state (Kafka topics persist across executions against a long-lived broker).
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
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    private String schemaHistoryTopic() {
        return serverName + ".schema-history";
    }

    /** Common smart-snapshot config: Kafka-backed schema history (shared across tasks) on the same broker. */
    private Configuration.Builder smartConfig() {
        return TestHelper.defaultConfig()
                .with(CommonConnectorConfig.TOPIC_PREFIX, serverName)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, true)
                .with("producer.override.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .with("admin.override.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .with(SqlServerConnectorConfig.SCHEMA_HISTORY, KafkaSchemaHistory.class)
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS)
                .with(KafkaSchemaHistory.TOPIC, schemaHistoryTopic())
                .with("tasks.max", 2);
    }

    /** Consumes the whole schema-history topic and concatenates the record values (DDL / table-change JSON). */
    private String schemaHistoryText() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-schema-history-reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        StringBuilder sb = new StringBuilder();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(schemaHistoryTopic()));
            long deadline = System.currentTimeMillis() + 15_000;
            int emptyPolls = 0;
            boolean any = false;
            while (System.currentTimeMillis() < deadline && emptyPolls < 3) {
                ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
                if (recs.isEmpty()) {
                    emptyPolls = any ? emptyPolls + 1 : emptyPolls;
                    continue;
                }
                any = true;
                emptyPolls = 0;
                for (ConsumerRecord<String, String> r : recs) {
                    sb.append(r.value()).append('\n');
                }
            }
        }
        return sb.toString();
    }

    @Test
    public void twoShardFanOutDispatchesEachTasksOwnShard() throws Exception {
        // tasks.max=2 over 2 tables -> 2 real shard tasks (task.id 0 and 1), each reading its own leader-assigned
        // shard from snapshot_info. task-0 is the leader (publishes snapshot_info) and the schema-history writer;
        // task-1 recovers its shard's structure from the schema-history topic (proven by its data appearing).
        Configuration config = smartConfig().build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(10);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table1")).hasSize(5);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table2")).hasSize(5);

        // task-0 owns the full schema history: both tables' CREATE TABLE come from it.
        String schemaHistory = schemaHistoryText();
        assertThat(schemaHistory).contains("table1");
        assertThat(schemaHistory).contains("table2");

        SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        SnapshotCoordinationFacade facade = new SnapshotCoordinationFacade(config, connectorConfig);
        try {
            facade.start(MissingTopicPolicy.ASSUME_EXISTS);
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
    public void twoShardFanOutUnderReadCommitted() throws Exception {
        // read_committed is the most popular SQL Server isolation mode. Smart snapshot supports it in Phase 0
        // alongside repeatable_read: cross-task consistency comes from the single L_db anchor + CDC catch-up +
        // last-writer-wins PK upsert, not from the snapshot isolation level, so the parallel fan-out must
        // produce the identical result under read_committed. Same 2-shard fan-out as
        // twoShardFanOutDispatchesEachTasksOwnShard, only with snapshot.isolation.mode=read_committed -- this
        // is what proves the isolation-mode gate relaxation is correct end-to-end (not just at config-validate).
        Configuration config = smartConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_ISOLATION_MODE, "read_committed")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(10);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table1")).hasSize(5);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table2")).hasSize(5);

        String schemaHistory = schemaHistoryText();
        assertThat(schemaHistory).contains("table1");
        assertThat(schemaHistory).contains("table2");

        SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        SnapshotCoordinationFacade facade = new SnapshotCoordinationFacade(config, connectorConfig);
        try {
            facade.start(MissingTopicPolicy.ASSUME_EXISTS);
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

        Configuration config = smartConfig()
                // SQL Server's table.exclude.list is schema.table (2-part), not database.schema.table.
                .with(RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST, "dbo\\.table3")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(10);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table1")).hasSize(5);
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table2")).hasSize(5);
        // Proves the exclusion took effect: table3 has schema but zero data (a silently-broken exclude would
        // capture it as a shard and this would fail).
        assertThat(records.recordsForTopic(serverName + ".testDB1.dbo.table3")).isNull();

        String schemaHistory = schemaHistoryText();
        assertThat(schemaHistory).contains("table1");
        assertThat(schemaHistory).contains("table2");
        assertThat(schemaHistory).contains("table3");

        stopConnector();
    }

    // NOTE: the "one task failure -> full restart" cycle (epoch bump driven by requestTaskReconfiguration) is
    // NOT tested here. Driving reconfiguration through the single embedded engine is inherently racy
    // (CREATING_TASKS vs STOPPING) and the full re-snapshot round can exceed the assertion window. The
    // restart_needed -> epoch-bump coordination is covered deterministically by
    // SmartSnapshotRestartCoordinationIT (this module) and SmartSnapshotConnectorCoordinatorTest (debezium-core);
    // the full reconfiguration-driven restart belongs to the real Connect-cluster (Tier-C) harness.
}
