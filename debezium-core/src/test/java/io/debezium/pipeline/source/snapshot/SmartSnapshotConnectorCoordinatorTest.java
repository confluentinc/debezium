/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.After;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigurationNames;
import io.debezium.pipeline.source.snapshot.SnapshotLifecycleManager.SnapshotSetup;
import io.debezium.relational.TableId;

/**
 * Connector-agnostic tests for the shared smart-snapshot orchestrator. Validates the fan-out and key-scoping
 * logic every connector (Postgres, MySQL, SQL Server) relies on, using the {@link InMemorySnapshotCoordination}
 * test double and mocked Connect context — no Kafka, DB, or Connect cluster required.
 * <p>
 * The full fan-out → monitor → downscale loop (which needs the runtime to honour
 * {@code requestTaskReconfiguration()}) is covered by the {@code EmbeddedConnectCluster}-based ITs.
 */
public class SmartSnapshotConnectorCoordinatorTest {

    private SmartSnapshotConnectorCoordinator coordinator;

    @After
    public void tearDown() {
        if (coordinator != null) {
            coordinator.stop();
        }
    }

    @Test
    public void fanOutDistributesAllTablesAcrossTasksTaggedWithEpochAndTaskId() {
        coordinator = new SmartSnapshotConnectorCoordinator(
                new InMemorySnapshotCoordination(), lifecycleReturning("snap", "100"), contextWithNoOffsets(), "srv");
        coordinator.start(tables(5), true);

        List<Map<String, String>> configs = coordinator.taskConfigs(3, new HashMap<>(), true);

        assertThat(configs).hasSize(3);
        Set<String> taskIds = new HashSet<>();
        Set<String> distributedTables = new HashSet<>();
        for (Map<String, String> cfg : configs) {
            assertThat(cfg.get(SmartSnapshotConnectorCoordinator.EPOCH_KEY)).isEqualTo("1");
            taskIds.add(cfg.get(ConfigurationNames.TASK_ID_PROPERTY_NAME));
            String shard = cfg.get(CommonConnectorConfig.SNAPSHOT_MODE_TABLES.name());
            assertThat(shard).isNotEmpty();
            distributedTables.addAll(Arrays.asList(shard.split(",")));
        }
        assertThat(taskIds).containsExactlyInAnyOrder("0", "1", "2");
        // every table is assigned to exactly one task (no loss, no duplication across shards)
        assertThat(distributedTables).hasSize(5);
    }

    @Test
    public void fanOutIsCappedByMaxTasks() {
        coordinator = new SmartSnapshotConnectorCoordinator(
                new InMemorySnapshotCoordination(), lifecycleReturning("snap", "100"), contextWithNoOffsets(), "srv");
        coordinator.start(tables(2), true);

        // maxTasks=5 but only 2 tables -> 2 tasks
        assertThat(coordinator.taskConfigs(5, new HashMap<>(), true)).hasSize(2);
    }

    @Test
    public void completedSnapshotInCoordinationTopicSkipsFanOut() {
        // A prior completed round (snapshot_completed=true on the shared coordination record) means there is
        // nothing left to snapshot: the coordinator reports complete and emits no multi-task configs.
        InMemorySnapshotCoordination coordination = new InMemorySnapshotCoordination();
        Map<String, Object> completed = new HashMap<>();
        completed.put("snapshot_completed", true);
        completed.put(SmartSnapshotConnectorCoordinator.EPOCH_KEY, 1);
        coordination.write(key("server", "srv"), completed);

        coordinator = new SmartSnapshotConnectorCoordinator(
                coordination, lifecycleReturning("snap", "100"), contextWithNoOffsets(), "srv");
        coordinator.start(tables(5), true);

        assertThat(coordinator.isComplete()).isTrue();
    }

    // ---- helpers ----

    private static SourceConnectorContext contextWithNoOffsets() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        when(reader.offset(anyMap())).thenReturn(null);
        SourceConnectorContext ctx = mock(SourceConnectorContext.class);
        when(ctx.offsetStorageReader()).thenReturn(reader);
        return ctx;
    }

    private static SnapshotLifecycleManager lifecycleReturning(String snapshotName, String consistentPosition) {
        SnapshotLifecycleManager lifecycle = mock(SnapshotLifecycleManager.class);
        when(lifecycle.prepareSnapshot(anyList(), anyBoolean())).thenReturn(new SnapshotSetup(snapshotName, consistentPosition));
        when(lifecycle.isValid()).thenReturn(true);
        when(lifecycle.consistentPosition()).thenReturn(consistentPosition);
        return lifecycle;
    }

    private static List<TableId> tables(int count) {
        List<TableId> tables = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            tables.add(new TableId("db", "schema", "t" + i));
        }
        return tables;
    }

    private static Map<String, String> key(String... kv) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put(kv[i], kv[i + 1]);
        }
        return map;
    }
}
