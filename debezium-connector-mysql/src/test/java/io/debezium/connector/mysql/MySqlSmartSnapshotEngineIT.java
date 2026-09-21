/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.kafka.KafkaCluster;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination.MissingTopicPolicy;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.util.Testing;

/**
 * Embedded-engine IT for MySQL smart snapshot (minimal locking). Uses a real MySQL, an in-memory Kafka for the
 * coordination topic, and the async embedded engine. Mirrors the Postgres {@code SmartSnapshotEngineIT}.
 */
public class MySqlSmartSnapshotEngineIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public final SkipTestRule skip = new SkipTestRule();

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-smart-snapshot.txt")
            .toAbsolutePath();

    private final UniqueDatabase DATABASE = new MySqlUniqueDatabase("smartit", "smart_snapshot_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

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

        DATABASE.createAndInitialize();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        initializeConnectorTestFramework();
    }

    @After
    public void after() throws Exception {
        try {
            stopConnector();
        }
        finally {
            if (kafka != null) {
                kafka.shutdown();
            }
            if (kafkaDir != null) {
                Testing.Files.delete(kafkaDir);
            }
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    public void multiTaskSnapshotThenDownscaleToStreaming() throws Exception {
        start(MySqlConnector.class, smartConfig().build());
        assertConnectorIsRunning();

        // snapshot phase: the two tasks together capture every row of both tables (disjoint subsets)
        SourceRecords snapshot = consumeRecordsByTopic(4);
        assertThat(snapshot.recordsForTopic(DATABASE.topicForTable("a"))).hasSize(2);
        assertThat(snapshot.recordsForTopic(DATABASE.topicForTable("b"))).hasSize(2);

        // after the monitor downscales to a single streaming task, a new insert must flow through via streaming.
        // Drain until the streamed row (id=100) shows up on topic a — tolerant of downscale/streaming-startup
        // latency and of any leftover records, unlike grabbing "the first 1 record".
        execute("INSERT INTO a VALUES (100, 'streamed');");
        BiPredicate<Integer, SourceRecord> untilStreamed = (n, rec) -> DATABASE.topicForTable("a").equals(rec.topic())
                && rec.key() instanceof Struct
                && Integer.valueOf(100).equals(((Struct) rec.key()).getInt32("id"));
        SourceRecords streamed = consumeRecordsByTopicUntil(untilStreamed);
        assertThat(keysFor(streamed, DATABASE.topicForTable("a"))).contains(100);
    }

    @Test
    public void restartOnRejoinReSnapshotsAtNewEpoch() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(SmartSnapshotConnectorCoordinator.class);

        // Pre-seed a stale started-transaction marker for task-0 at epoch 1. On start, task-0 sees its own marker
        // and treats it as a rejoin (its consistent-snapshot transaction can't be resumed), so it signals
        // restart_needed; the monitor then bumps the epoch and the engine restarts the tasks at epoch 2.
        seedStaleTransactionStartedMarker();

        start(MySqlConnector.class, smartConfig().build());
        assertConnectorIsRunning();

        // a post-start change must survive the restart (captured by the epoch-2 re-snapshot or by streaming)
        execute("INSERT INTO a VALUES (100, 'after-restart');");

        // drain everything until the marker row shows up on table a (tolerant of the re-snapshot's duplicates)
        BiPredicate<Integer, SourceRecord> untilMarker = (n, rec) -> DATABASE.topicForTable("a").equals(rec.topic())
                && rec.key() instanceof Struct
                && Integer.valueOf(100).equals(((Struct) rec.key()).getInt32("id"));
        SourceRecords records = consumeRecordsByTopicUntil(untilMarker);

        // the epoch-bump restart actually happened
        assertThat(logInterceptor.containsMessage("Restart needed, epoch bumped from 1 to 2")).isTrue();

        // and the connector recovered: after dedup-by-key, table a has the base rows plus the post-restart row
        // (record-level duplicates from the re-snapshot are expected and collapse under key dedup)
        assertThat(keysFor(records, DATABASE.topicForTable("a"))).contains(1, 2, 100);
    }

    @Test
    public void leaderWritesCompleteSchemaHistoryExactlyOnce() throws Exception {
        // Keep the public schema-change topic ON so the single-writer guarantee is observable: if a follower
        // wrongly persisted/emitted schema, a table's CREATE TABLE would appear more than once.
        start(MySqlConnector.class, smartConfigBase().with("include.schema.changes", true).build());
        assertConnectorIsRunning();

        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());

        // The leader writes the full schema history for ALL tables BEFORE any data row is emitted, so draining
        // until both tables' data rows have arrived guarantees every schema-change record has been consumed too.
        int[] aRows = { 0 };
        int[] bRows = { 0 };
        BiPredicate<Integer, SourceRecord> untilAllData = (n, rec) -> {
            if (DATABASE.topicForTable("a").equals(rec.topic())) {
                aRows[0]++;
            }
            else if (DATABASE.topicForTable("b").equals(rec.topic())) {
                bRows[0]++;
            }
            return aRows[0] >= 2 && bRows[0] >= 2;
        };
        SourceRecords records = consumeRecordsByTopicUntil(untilAllData);
        records.allRecordsInOrder().forEach(schemaChanges::add);

        // The leader emitted schema history, and each table's CREATE TABLE appears EXACTLY once (single writer).
        assertThat(schemaChanges.recordCount()).isGreaterThan(0);
        assertThat(createTableCount(schemaChanges, "a")).isEqualTo(1);
        assertThat(createTableCount(schemaChanges, "b")).isEqualTo(1);
    }

    @Test
    public void tableSplitCapturesEveryRowExactlyOnce() throws Exception {
        // Add two more tables so the round-robin split spreads 4 tables across 2 tasks.
        execute("CREATE TABLE c (id INT NOT NULL PRIMARY KEY, v VARCHAR(64));",
                "INSERT INTO c VALUES (1, 'c1'), (2, 'c2');",
                "CREATE TABLE d (id INT NOT NULL PRIMARY KEY, v VARCHAR(64));",
                "INSERT INTO d VALUES (1, 'd1'), (2, 'd2');");

        start(MySqlConnector.class, smartConfig().build());
        assertConnectorIsRunning();

        // Every row of every table is captured exactly once (disjoint, complete split): 4 tables x 2 rows.
        SourceRecords snapshot = consumeRecordsByTopic(8);
        assertThat(snapshot.recordsForTopic(DATABASE.topicForTable("a"))).hasSize(2);
        assertThat(snapshot.recordsForTopic(DATABASE.topicForTable("b"))).hasSize(2);
        assertThat(snapshot.recordsForTopic(DATABASE.topicForTable("c"))).hasSize(2);
        assertThat(snapshot.recordsForTopic(DATABASE.topicForTable("d"))).hasSize(2);
    }

    @Test
    public void initialOnlySnapshotsAcrossTasks() throws Exception {
        // initial_only: parallel data snapshot, no streaming afterwards. Every row is still captured.
        start(MySqlConnector.class, smartConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL_ONLY.getValue())
                .build());
        assertConnectorIsRunning();

        SourceRecords snapshot = consumeRecordsByTopic(4);
        assertThat(snapshot.recordsForTopic(DATABASE.topicForTable("a"))).hasSize(2);
        assertThat(snapshot.recordsForTopic(DATABASE.topicForTable("b"))).hasSize(2);
    }

    @Test
    public void nonDefaultLockingModeFallsBackToSingleTask() throws Exception {
        // We only support the default (minimal) locking mode for smart snapshot. With any other mode the feature
        // must NOT engage: the connector runs a normal single-task snapshot and still captures all rows.
        LogInterceptor coordinatorLog = new LogInterceptor(SmartSnapshotConnectorCoordinator.class);

        start(MySqlConnector.class, smartConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.EXTENDED)
                .build());
        assertConnectorIsRunning();

        SourceRecords snapshot = consumeRecordsByTopic(4);
        assertThat(snapshot.recordsForTopic(DATABASE.topicForTable("a"))).hasSize(2);
        assertThat(snapshot.recordsForTopic(DATABASE.topicForTable("b"))).hasSize(2);

        // smart snapshot did not engage: no coordinator monitor was started
        assertThat(coordinatorLog.containsMessage("Monitor thread started")).isFalse();
    }

    @Test
    public void snapshotToStreamingIsExactlyOnceUnderWrites() throws Exception {
        start(MySqlConnector.class, smartConfig().build());
        assertConnectorIsRunning();

        // Writes that race the parallel snapshot and/or land during streaming. Whichever side of the boundary each
        // falls on, the reconstructed final state must exactly equal the source (no lost or duplicated effect).
        // A sentinel row (id=999) is inserted into BOTH tables last; draining until both sentinels are seen
        // guarantees both tables are fully consumed before we diff (a single-table sentinel could stop early,
        // since the two tables are snapshotted by different tasks and interleave arbitrarily).
        execute("INSERT INTO a VALUES (3, 'a3');",
                "UPDATE a SET v = 'a1x' WHERE id = 1;",
                "DELETE FROM a WHERE id = 2;",
                "UPDATE b SET v = 'b2x' WHERE id = 2;",
                "INSERT INTO b VALUES (3, 'b3');",
                "INSERT INTO a VALUES (999, 'sa');",
                "INSERT INTO b VALUES (999, 'sb');");

        final boolean[] seenA = { false };
        final boolean[] seenB = { false };
        BiPredicate<Integer, SourceRecord> untilBothSentinels = (n, rec) -> {
            if (rec.key() instanceof Struct && Integer.valueOf(999).equals(((Struct) rec.key()).getInt32("id"))) {
                seenA[0] |= DATABASE.topicForTable("a").equals(rec.topic());
                seenB[0] |= DATABASE.topicForTable("b").equals(rec.topic());
            }
            return seenA[0] && seenB[0];
        };
        SourceRecords records = consumeRecordsByTopicUntil(untilBothSentinels);

        // dedup-by-PK diff against the live source — the exactly-once / no-overlap guarantee
        assertThat(reconstruct(records, DATABASE.topicForTable("a"))).isEqualTo(readTable("a"));
        assertThat(reconstruct(records, DATABASE.topicForTable("b"))).isEqualTo(readTable("b"));
    }

    @Test
    public void streamingRecoversSchemaAndAppliesDdlAfterDownscale() throws Exception {
        start(MySqlConnector.class, smartConfig().build());
        assertConnectorIsRunning();

        // let the parallel snapshot complete
        consumeRecordsByTopic(4);

        // After downscale, the streaming task must have recovered the leader-written schema (else it fails at
        // assureNonEmptySchema) AND keep applying binlog DDL: ALTER then insert a row using the new column.
        execute("ALTER TABLE a ADD COLUMN w VARCHAR(32) NULL;",
                "INSERT INTO a VALUES (200, 'v200', 'w200');");

        BiPredicate<Integer, SourceRecord> untilAltered = (n, rec) -> DATABASE.topicForTable("a").equals(rec.topic())
                && rec.key() instanceof Struct
                && Integer.valueOf(200).equals(((Struct) rec.key()).getInt32("id"));
        SourceRecords records = consumeRecordsByTopicUntil(untilAltered);

        SourceRecord altered = records.recordsForTopic(DATABASE.topicForTable("a")).stream()
                .filter(r -> r.key() instanceof Struct && Integer.valueOf(200).equals(((Struct) r.key()).getInt32("id")))
                .reduce((first, second) -> second) // last one wins
                .orElseThrow(() -> new AssertionError("streamed row id=200 not found"));
        Struct after = ((Struct) altered.value()).getStruct("after");
        assertThat(after.schema().field("w")).as("new column recovered/applied during streaming").isNotNull();
        assertThat(after.getString("w")).isEqualTo("w200");
    }

    @Test
    public void tasksMaxOneWithSmartEnabledRunsSingleTask() throws Exception {
        LogInterceptor coordinatorLog = new LogInterceptor(SmartSnapshotConnectorCoordinator.class);

        // smart snapshot needs >1 task; with tasks.max=1 it must not engage, just a normal single-task snapshot.
        start(MySqlConnector.class, smartConfig().with("tasks.max", 1).build());
        assertConnectorIsRunning();

        SourceRecords snapshot = consumeRecordsByTopic(4);
        assertThat(snapshot.recordsForTopic(DATABASE.topicForTable("a"))).hasSize(2);
        assertThat(snapshot.recordsForTopic(DATABASE.topicForTable("b"))).hasSize(2);
        assertThat(coordinatorLog.containsMessage("Monitor thread started")).isFalse();
    }

    @Test
    public void restartOnRejoinReSnapshotsWithCorrectData() throws Exception {
        LogInterceptor coordinatorLog = new LogInterceptor(SmartSnapshotConnectorCoordinator.class);
        // Force an epoch bump on task-0's rejoin (its consistent-snapshot txn can't be resumed).
        seedStaleTransactionStartedMarker();

        start(MySqlConnector.class, smartConfig().build());
        assertConnectorIsRunning();

        execute("INSERT INTO a VALUES (3, 'a3');",
                "DELETE FROM a WHERE id = 2;",
                "INSERT INTO a VALUES (999, 'sentinel');");

        BiPredicate<Integer, SourceRecord> untilSentinel = (n, rec) -> DATABASE.topicForTable("a").equals(rec.topic())
                && rec.key() instanceof Struct
                && Integer.valueOf(999).equals(((Struct) rec.key()).getInt32("id"));
        SourceRecords records = consumeRecordsByTopicUntil(untilSentinel);

        // the restart actually happened, and after dedup the re-snapshotted data matches the source exactly
        assertThat(coordinatorLog.containsMessage("Restart needed, epoch bumped from 1 to 2")).isTrue();
        assertThat(reconstruct(records, DATABASE.topicForTable("a"))).isEqualTo(readTable("a"));
    }

    @Test
    public void restartAfterCompletionResumesStreamingWithoutResnapshot() throws Exception {
        LogInterceptor coordinatorLog = new LogInterceptor(SmartSnapshotConnectorCoordinator.class);

        // Run 1: full smart snapshot -> downscale -> stream one record so a streaming offset is committed.
        start(MySqlConnector.class, smartConfig().build());
        assertConnectorIsRunning();
        consumeRecordsByTopic(4); // snapshot a:2, b:2
        execute("INSERT INTO a VALUES (100, 'streamed');");
        BiPredicate<Integer, SourceRecord> until100 = (n, rec) -> DATABASE.topicForTable("a").equals(rec.topic())
                && rec.key() instanceof Struct
                && Integer.valueOf(100).equals(((Struct) rec.key()).getInt32("id"));
        consumeRecordsByTopicUntil(until100);

        stopConnector();

        // Run 2: restart with the same config; the committed streaming offset must make the connector skip smart
        // snapshot and resume streaming — NOT take a second snapshot.
        start(MySqlConnector.class, smartConfig().build());
        assertConnectorIsRunning();
        execute("INSERT INTO a VALUES (101, 'after-restart');");
        BiPredicate<Integer, SourceRecord> until101 = (n, rec) -> DATABASE.topicForTable("a").equals(rec.topic())
                && rec.key() instanceof Struct
                && Integer.valueOf(101).equals(((Struct) rec.key()).getInt32("id"));
        SourceRecords resumed = consumeRecordsByTopicUntil(until101);

        // the connector recognized the existing streaming offset and skipped smart snapshot
        assertThat(coordinatorLog.containsMessage("Existing streaming offset present, skipping smart snapshot")).isTrue();
        // the new row streamed, and the resumed run did NOT re-snapshot (no snapshot 'read' records)
        assertThat(keysFor(resumed, DATABASE.topicForTable("a"))).contains(101);
        for (SourceRecord r : resumed.recordsForTopic(DATABASE.topicForTable("a"))) {
            assertThat(((Struct) r.value()).getString("op")).as("resumed run must stream, not re-snapshot").isNotEqualTo("r");
        }
    }

    private static int createTableCount(SchemaChangeHistory history, String table) {
        int[] count = { 0 };
        history.forEach(rec -> {
            if (rec.value() instanceof Struct) {
                Struct value = (Struct) rec.value();
                if (value.schema().field("ddl") == null) {
                    return;
                }
                String ddl = value.getString("ddl");
                if (ddl != null && ddl.toUpperCase().contains("CREATE TABLE") && ddl.contains("`" + table + "`")) {
                    count[0]++;
                }
            }
        });
        return count[0];
    }

    private Configuration.Builder smartConfigBase() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL.getValue())
                .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.MINIMAL)
                .with("tasks.max", 2)
                .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, true)
                .with("producer.override.bootstrap.servers", kafka.brokerList())
                .with("admin.override.bootstrap.servers", kafka.brokerList())
                // short monitor poll so the downscale/restart happens in seconds instead of the 30s default
                .with(CommonConnectorConfig.SMART_SNAPSHOT_MONITOR_POLL_INTERVAL_MS, 2000);
    }

    private Configuration.Builder smartConfig() {
        // count-sensitive tests suppress the public schema-change topic so only data records are emitted
        return smartConfigBase().with("include.schema.changes", false);
    }

    private void seedStaleTransactionStartedMarker() {
        // Reuse the full connector config (via smartConfig) so MySqlConnectorConfig can initialize its JDBC
        // credentials provider — a bare Configuration lacks the jdbc.creds.provider.* settings and fails.
        Configuration cfg = smartConfig().build();
        SnapshotCoordinationFacade facade = new SnapshotCoordinationFacade(cfg, new MySqlConnectorConfig(cfg));
        facade.start(MissingTopicPolicy.ASSUME_EXISTS);
        facade.writeTaskStartedTransaction("0", 1);
        facade.stop();
    }

    private void execute(String... statements) throws SQLException {
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            db.execute(statements);
        }
    }

    /** The current (id -&gt; v) state of a source table, read directly from MySQL. */
    private Map<Integer, String> readTable(String table) throws SQLException {
        Map<Integer, String> rows = new HashMap<>();
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            db.query("SELECT id, v FROM " + table + " ORDER BY id", rs -> {
                while (rs.next()) {
                    rows.put(rs.getInt(1), rs.getString(2));
                }
            });
        }
        return rows;
    }

    /**
     * Reconstruct the per-PK final (id -&gt; v) state from the CDC records on a topic by applying create/read/update
     * (put) and delete (remove) in record order. Collapses any cross-epoch/snapshot duplicates, so the result can be
     * diffed against {@link #readTable} to assert exactly-once / no-loss across the snapshot→streaming boundary.
     */
    private static Map<Integer, String> reconstruct(SourceRecords records, String topic) {
        Map<Integer, String> state = new HashMap<>();
        List<SourceRecord> forTopic = records.recordsForTopic(topic);
        if (forTopic == null) {
            return state;
        }
        for (SourceRecord r : forTopic) {
            if (!(r.key() instanceof Struct) || r.value() == null) {
                continue; // tombstone or non-struct key
            }
            Integer id = ((Struct) r.key()).getInt32("id");
            Struct value = (Struct) r.value();
            if ("d".equals(value.getString("op"))) {
                state.remove(id);
            }
            else {
                Struct after = value.getStruct("after");
                state.put(id, after == null ? null : after.getString("v"));
            }
        }
        return state;
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
