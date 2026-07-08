/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.kafka.KafkaCluster;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.util.Testing;

public class SmartSnapshotEngineIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public final SkipTestRule skip = new SkipTestRule();

    private KafkaCluster kafka;
    private File kafkaDir;

    @Before
    public void before() throws Exception {
        // in-memory Kafka only for the smart-snapshot coordination topic
        kafkaDir = Testing.Files.createTestingDirectory("smart-snapshot-coordination");
        kafka = new KafkaCluster().usingDirectory(kafkaDir)
                .deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true)
                .addBrokers(1)
                .startup();

        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE SCHEMA s1;");
        TestHelper.execute("CREATE TABLE s1.a (id int PRIMARY KEY, v text);"
                + "INSERT INTO s1.a VALUES (1, 'a1'), (2, 'a2');"
                + "CREATE TABLE s1.b (id int PRIMARY KEY, v text);"
                + "INSERT INTO s1.b VALUES (1, 'b1'), (2, 'b2');");

        initializeConnectorTestFramework();
    }

    @After
    public void after() throws Exception {
        stopConnector();
        if (kafka != null) {
            kafka.shutdown();
        }
        if (kafkaDir != null) {
            Testing.Files.delete(kafkaDir);
        }
        TestHelper.dropDefaultReplicationSlot();
    }

    @Test
    public void multiTaskSnapshotThenDownscaleToStreaming() throws Exception {
        start(PostgresConnector.class, smartConfig().build());
        assertConnectorIsRunning();

        // snapshot phase: the two tasks together capture every row of both tables (disjoint subsets)
        SourceRecords snapshot = consumeRecordsByTopic(4);
        assertThat(snapshot.recordsForTopic(topicName("s1.a"))).hasSize(2);
        assertThat(snapshot.recordsForTopic(topicName("s1.b"))).hasSize(2);

        // after the monitor downscales to a single streaming task, a new insert must flow through via streaming
        TestHelper.execute("INSERT INTO s1.a VALUES (100, 'streamed');");
        SourceRecords streamed = consumeRecordsByTopic(1);
        assertThat(streamed.recordsForTopic(topicName("s1.a"))).hasSize(1);
    }

    @Test
    public void restartOnRejoinReSnapshotsAtNewEpoch() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(SmartSnapshotConnectorCoordinator.class);

        // Pre-seed a stale join marker for task-0 for the epoch 1. On start, task-0 sees its own join marker and
        // treats it as a rejoin (its exported snapshot transaction can't be resumed), so it signals
        // restart_needed; the monitor then bumps the epoch and the engine restarts the tasks at epoch 1.
        seedStaleJoinMarker();

        start(PostgresConnector.class, smartConfig().build());
        assertConnectorIsRunning();

        // a post-start change must survive the restart (captured by the epoch-1 re-snapshot or by streaming)
        TestHelper.execute("INSERT INTO s1.a VALUES (100, 'after-restart');");

        // drain everything until the marker row shows up on s1.a (tolerant of the re-snapshot's duplicates)
        BiPredicate<Integer, SourceRecord> untilMarker = (n, rec) -> topicName("s1.a").equals(rec.topic())
                && rec.key() instanceof Struct
                && Integer.valueOf(100).equals(((Struct) rec.key()).getInt32("id"));
        SourceRecords records = consumeRecordsByTopicUntil(untilMarker);

        // the epoch-bump restart actually happened
        assertThat(logInterceptor.containsMessage("Epoch restart 1 -> 2")).isTrue();

        // and the connector recovered: after dedup-by-key, s1.a has the base rows plus the post-restart row
        // (record-level duplicates from the re-snapshot are expected and collapse under key dedup)
        assertThat(keysFor(records, topicName("s1.a"))).contains(1, 2, 100);
    }

    private Configuration.Builder smartConfig() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.HOSTNAME, "localhost")
                .with(PostgresConnectorConfig.PORT, 5432)
                .with(PostgresConnectorConfig.USER, "postgres")
                .with(PostgresConnectorConfig.PASSWORD, "postgres")
                .with(PostgresConnectorConfig.DATABASE_NAME, "postgres")
                .with("tasks.max", 2)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, true)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_COORDINATION_BOOTSTRAP_SERVERS, kafka.brokerList())
                // short monitor poll so the downscale/restart happens in seconds instead of the 30s default
                .with(CommonConnectorConfig.SMART_SNAPSHOT_MONITOR_POLL_INTERVAL_MS, 2000);
    }

    private void seedStaleJoinMarker() {
        Configuration cfg = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, TestHelper.TEST_SERVER)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_COORDINATION_BOOTSTRAP_SERVERS, kafka.brokerList())
                .build();
        SnapshotCoordinationFacade facade = new SnapshotCoordinationFacade(cfg, new PostgresConnectorConfig(cfg));
        facade.start();
        facade.writeJoin("0", 1);
        facade.stop();
    }

    private static Set<Integer> keysFor(SourceRecords records, String topic) {
        Set<Integer> keys = new HashSet<>();
        List<SourceRecord> forTopic = records.recordsForTopic(topic);
        if (forTopic != null) {
            for (SourceRecord r : forTopic) {
                if (r.key() instanceof Struct) {
                    keys.add(((Struct) r.key()).getInt32("id"));
                }
            }
        }
        return keys;
    }
}
