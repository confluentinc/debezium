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
import io.debezium.pipeline.source.snapshot.SmartSnapshotLeader;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination.MissingTopicPolicy;
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
        seedStaleTransactionStartedMarker();

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
        assertThat(logInterceptor.containsMessage("Restart needed, epoch bumped from 1 to 2")).isTrue();

        // and the connector recovered: after dedup-by-key, s1.a has the base rows plus the post-restart row
        // (record-level duplicates from the re-snapshot are expected and collapse under key dedup)
        assertThat(keysFor(records, topicName("s1.a"))).contains(1, 2, 100);
    }

    /**
     * The leader's held snapshot-holder connection is killed while it is waiting for tasks to start their
     * transaction. keepAlive() then throws, the leader releases the locks, signals a restart (bumping the epoch),
     * closes its coordination facade cleanly, and the retried round completes the snapshot.
     * <p>
     * The window is made deterministic with {@code snapshot.delay.ms}: that delay runs before each task attaches
     * (before SET TRANSACTION SNAPSHOT / schema read / started_transaction), so while the tasks are stalled the
     * leader sits in its started-transaction wait and only its own held connection is 'idle in transaction'. A
     * short leader poll interval makes keepAlive notice the kill within ~1s.
     */
    @Test
    public void snapshotHolderConnectionKilledMidRoundRecovers() throws Exception {
        LogInterceptor leaderLog = new LogInterceptor(SmartSnapshotLeader.class);
        LogInterceptor monitorLog = new LogInterceptor(SmartSnapshotConnectorCoordinator.class);

        Configuration config = smartConfig()
                .with(CommonConnectorConfig.SNAPSHOT_DELAY_MS, 5000)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_LEADER_POLL_INTERVAL_MS, 500)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        // Terminate the leader's held snapshot connection. We target by state = 'idle in transaction' rather than
        // by table locks, because snapshot.locking.mode defaults to NONE — the leader holds NO relation locks, so
        // a lock-based filter would match nothing. During this window the only idle-in-transaction backends are
        // the leader's held connections: the tasks are stalled in snapshot.delay (not yet attached) and the
        // streaming/replication connections are not idle-in-transaction. Scoping to the current database is safe
        // for these ITs, which assume exclusive DB access anyway (before() runs dropAllSchemas()); they are not
        // designed to run in parallel against a shared Postgres.
        awaitLog(leaderLog, "Prepared snapshot");
        TestHelper.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                + "WHERE datname = current_database() AND state = 'idle in transaction' AND pid <> pg_backend_pid();");

        // keepAlive throws -> leader releases + signals restart -> monitor bumps the epoch
        awaitLog(monitorLog, "Restart needed, epoch bumped");

        // Prove the retried round captured BOTH tables. A snapshot row (e.g. s1.b id=2) is not a safe gate: the
        // two tables are snapshotted by different tasks in parallel, so their rows interleave arbitrarily and one
        // table's rows may not be consumed yet when the other's arrive. Instead insert a high-PK marker into each
        // table; whether the round grabs it in its snapshot or via streaming, that marker arrives after that
        // table's base rows, so waiting for BOTH markers guarantees both tables were fully re-snapshotted.
        TestHelper.execute("INSERT INTO s1.a VALUES (100, 'after-kill'); INSERT INTO s1.b VALUES (100, 'after-kill');");
        Set<String> markedTopics = new HashSet<>();
        BiPredicate<Integer, SourceRecord> untilBothMarkers = (n, rec) -> {
            if (rec.key() instanceof Struct && Integer.valueOf(100).equals(((Struct) rec.key()).getInt32("id"))) {
                markedTopics.add(rec.topic());
            }
            return markedTopics.contains(topicName("s1.a")) && markedTopics.contains(topicName("s1.b"));
        };
        SourceRecords records = consumeRecordsByTopicUntil(untilBothMarkers);

        // dedup by key tolerates any cross-epoch record duplicates
        assertThat(keysFor(records, topicName("s1.a"))).contains(1, 2, 100);
        assertThat(keysFor(records, topicName("s1.b"))).contains(1, 2, 100);
    }

    private static void awaitLog(LogInterceptor log, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!log.containsMessage(message) && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        assertThat(log.containsMessage(message)).as("expected log message: " + message).isTrue();
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
                .with("producer.override.bootstrap.servers", kafka.brokerList())
                .with("admin.override.bootstrap.servers", kafka.brokerList())
                // short monitor poll so the downscale/restart happens in seconds instead of the 30s default
                .with(CommonConnectorConfig.SMART_SNAPSHOT_MONITOR_POLL_INTERVAL_MS, 2000);
    }

    private void seedStaleTransactionStartedMarker() {
        Configuration cfg = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, TestHelper.TEST_SERVER)
                .with("producer.override.bootstrap.servers", kafka.brokerList())
                .with("admin.override.bootstrap.servers", kafka.brokerList())
                .build();
        SnapshotCoordinationFacade facade = new SnapshotCoordinationFacade(cfg, new PostgresConnectorConfig(cfg));
        facade.start(SnapshotCoordination.MissingTopicPolicy.ASSUME_EXISTS);
        facade.writeTaskStartedTransaction("0", 1);
        facade.stop();
    }

    private static Set<Integer> keysFor(SourceRecords records, String topic) {
        Set<Integer> keys = new HashSet<>();
        List<SourceRecord> forTopic = records.recordsForTopic(topic);
        System.out.println("printing the records for topic " + topic);
        if (forTopic != null) {
            for (SourceRecord r : forTopic) {
                System.out.println("XXX " + r);
                if (r.key() instanceof Struct) {
                    keys.add(((Struct) r.key()).getInt32("id"));
                }
            }
        }
        return keys;
    }
}
