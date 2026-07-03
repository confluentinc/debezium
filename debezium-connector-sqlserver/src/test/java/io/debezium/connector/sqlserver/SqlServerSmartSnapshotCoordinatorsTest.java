/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.relational.TableId;

public class SqlServerSmartSnapshotCoordinatorsTest {

    /** In-memory {@link SnapshotCoordination} double -- no Kafka needed. */
    private static class InMemorySnapshotCoordination implements SnapshotCoordination {
        private final Map<Map<String, String>, Map<String, Object>> store = new ConcurrentHashMap<>();

        @Override
        public void write(Map<String, String> key, Map<String, Object> data) {
            store.put(key, new HashMap<>(data));
        }

        @Override
        public Map<String, Object> read(Map<String, String> key) {
            return store.get(key);
        }

        @Override
        public Map<String, Object> readSync(Map<String, String> key) {
            return read(key);
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }
    }

    /** No pre-existing offsets -- every database looks "never streamed before." */
    private static SourceConnectorContext noOffsetsContext() {
        OffsetStorageReader reader = new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(Map<String, T> partition) {
                return null;
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                return Collections.emptyMap();
            }
        };
        return new SourceConnectorContext() {
            @Override
            public OffsetStorageReader offsetStorageReader() {
                return reader;
            }

            @Override
            public void requestTaskReconfiguration() {
            }

            @Override
            public void raiseError(Exception e) {
                throw new AssertionError("unexpected connector error", e);
            }
        };
    }

    /**
     * A genuine pre-existing streaming offset for {@code streamingDatabase} (no {@code snapshot} key --
     * matches what {@link SqlServerOffsetContext#getOffset()} produces once past the initial snapshot), and
     * nothing for any other database.
     */
    private static SourceConnectorContext alreadyStreamingContext(String serverName, String streamingDatabase) {
        Map<String, String> streamingPartition = new SqlServerPartition(serverName, streamingDatabase).getSourcePartition();
        OffsetStorageReader reader = new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(Map<String, T> partition) {
                return streamingPartition.equals(partition) ? Map.of("commit_lsn", "0000002b:00000ce8:001a") : null;
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                return Collections.emptyMap();
            }
        };
        return new SourceConnectorContext() {
            @Override
            public OffsetStorageReader offsetStorageReader() {
                return reader;
            }

            @Override
            public void requestTaskReconfiguration() {
            }

            @Override
            public void raiseError(Exception e) {
                throw new AssertionError("unexpected connector error", e);
            }
        };
    }

    private SqlServerConnectorConfig configFor(int databaseCount, int tablesPerTask) {
        StringBuilder databaseNames = new StringBuilder();
        for (int i = 0; i < databaseCount; i++) {
            if (i > 0) {
                databaseNames.append(",");
            }
            databaseNames.append("db").append(i);
        }
        return new SqlServerConnectorConfig(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                .with(SqlServerConnectorConfig.DATABASE_NAMES, databaseNames.toString())
                .with(CommonConnectorConfig.SMART_SNAPSHOT_TABLES_PER_TASK, tablesPerTask)
                .build());
    }

    /** A connection stub that answers table listings and LSN capture without touching the network. */
    private static class StubConnection extends SqlServerConnection {
        private final Map<String, Integer> tableCountByDatabase;

        StubConnection(SqlServerConnectorConfig config, Map<String, Integer> tableCountByDatabase) {
            super(config, null, Collections.emptySet(), config.useSingleDatabase());
            this.tableCountByDatabase = tableCountByDatabase;
        }

        @Override
        public Set<TableId> readTableNames(String databaseName, String schemaNamePattern, String tableNamePattern, String[] tableTypes) {
            int count = tableCountByDatabase.getOrDefault(databaseName, 0);
            return java.util.stream.IntStream.range(0, count)
                    .mapToObj(i -> new TableId(databaseName, "dbo", "t" + i))
                    .collect(Collectors.toCollection(java.util.LinkedHashSet::new));
        }

        @Override
        public Lsn getMaxLsn(String databaseName) {
            return Lsn.valueOf(new byte[]{ 0x01 });
        }

        @Override
        public synchronized void close() {
            // no real connection was ever opened
        }
    }

    @Test
    public void taskConfigsAppliesTableDrivenAllocationAndRelabelsIndexPerDatabase() {
        // db0: 3 tables, tablesPerTask=2 -> ceil(3/2)=2 shards; db1: 1 table -> 1 shard
        SqlServerConnectorConfig config = configFor(2, 2);
        Map<String, Integer> tableCounts = Map.of("db0", 3, "db1", 1);
        StubConnection connection = new StubConnection(config, tableCounts);

        SqlServerSmartSnapshotCoordinators coordinators = new SqlServerSmartSnapshotCoordinators();
        coordinators.start(config, noOffsetsContext(), () -> connection, database -> new InMemorySnapshotCoordination());

        assertThat(coordinators.hasActiveDatabases()).isTrue();
        assertThat(coordinators.remainingDatabases(config)).isEmpty();

        List<Map<String, String>> configs = coordinators.taskConfigs(
                config.getSmartSnapshotTablesPerTask(), Map.of());

        assertThat(configs).hasSize(3); // 2 shards for db0 + 1 shard for db1

        Map<String, List<Map<String, String>>> byDatabase = configs.stream()
                .collect(Collectors.groupingBy(c -> c.get(SqlServerConnectorConfig.DATABASE_NAMES.name())));
        assertThat(byDatabase.get("db0")).hasSize(2);
        assertThat(byDatabase.get("db1")).hasSize(1);

        // every sharded config carries the per-DB coordination index, distinct from (and here, coincidentally
        // equal per-DB to) the shared coordinator's own local task.id numbering
        for (Map<String, String> c : configs) {
            assertThat(c).containsKey(SqlServerConnectorConfig.SMART_SNAPSHOT_DATABASE_TASK_INDEX_PROPERTY_NAME);
        }
        List<String> db0Indices = byDatabase.get("db0").stream()
                .map(c -> c.get(SqlServerConnectorConfig.SMART_SNAPSHOT_DATABASE_TASK_INDEX_PROPERTY_NAME))
                .sorted()
                .collect(Collectors.toList());
        assertThat(db0Indices).containsExactly("0", "1");
    }

    @Test
    public void databaseWithNoCapturedTablesIsExcludedAndLeftForTheOrdinaryPath() {
        SqlServerConnectorConfig config = configFor(2, 2);
        Map<String, Integer> tableCounts = Map.of("db0", 2, "db1", 0);
        StubConnection connection = new StubConnection(config, tableCounts);

        SqlServerSmartSnapshotCoordinators coordinators = new SqlServerSmartSnapshotCoordinators();
        coordinators.start(config, noOffsetsContext(), () -> connection, database -> new InMemorySnapshotCoordination());

        assertThat(coordinators.remainingDatabases(config)).containsExactly("db1");

        List<Map<String, String>> configs = coordinators.taskConfigs(config.getSmartSnapshotTablesPerTask(), Map.of());
        assertThat(configs).allSatisfy(c -> assertThat(c.get(SqlServerConnectorConfig.DATABASE_NAMES.name())).isEqualTo("db0"));
    }

    @Test
    public void databaseAlreadyStreamingIsExcludedFromSmartSnapshotEvenThoughSharedCheckCannotSeeIt() {
        // The shared SmartSnapshotConnectorCoordinator's own "already streaming" check looks up a
        // single-field {server} key against the real offset store, which never matches SqlServerPartition's
        // {server,database} key -- SqlServerSmartSnapshotCoordinators must do its own two-field check
        // before ever constructing a coordinator for a database that's already genuinely streaming.
        SqlServerConnectorConfig config = configFor(2, 2);
        Map<String, Integer> tableCounts = Map.of("db0", 2, "db1", 2);
        StubConnection connection = new StubConnection(config, tableCounts);

        SqlServerSmartSnapshotCoordinators coordinators = new SqlServerSmartSnapshotCoordinators();
        coordinators.start(config, alreadyStreamingContext(config.getLogicalName(), "db0"), () -> connection,
                database -> new InMemorySnapshotCoordination());

        assertThat(coordinators.remainingDatabases(config)).containsExactly("db0");

        List<Map<String, String>> configs = coordinators.taskConfigs(config.getSmartSnapshotTablesPerTask(), Map.of());
        assertThat(configs).allSatisfy(c -> assertThat(c.get(SqlServerConnectorConfig.DATABASE_NAMES.name())).isEqualTo("db1"));
    }
}
