/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.config.ConfigurationNames;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

/**
 * Unit tests for {@link SmartSnapshotConnectorCoordinator}: the deterministic table split, the parser, the
 * epoch/state machine in {@code taskConfigs}, the {@code start} decision, and a single monitor iteration.
 */
public class SmartSnapshotConnectorCoordinatorTest {

    @Mock
    private SnapshotCoordinationFacade facade;

    @Mock
    private SourceConnectorContext connectorContext;

    @Mock
    private OffsetStorageReader offsetStorageReader;

    private SmartSnapshotConnectorCoordinator coordinator;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);
        coordinator = new SmartSnapshotConnectorCoordinator(facade, connectorContext, "srv", 30_000L);
    }

    @After
    public void after() {
        coordinator.stop();
    }

    @Test
    public void tablesForTaskSplitsRoundRobinAfterStableSort() {
        List<TableId> all = List.of(
                new TableId(null, "public", "d"),
                new TableId(null, "public", "b"),
                new TableId(null, "public", "a"),
                new TableId(null, "public", "c"));
        // sorted: a, b, c, d
        assertThat(SmartSnapshotConnectorCoordinator.tablesForTask(all, 0, 2))
                .containsExactly(new TableId(null, "public", "a"), new TableId(null, "public", "c"));
        assertThat(SmartSnapshotConnectorCoordinator.tablesForTask(all, 1, 2))
                .containsExactly(new TableId(null, "public", "b"), new TableId(null, "public", "d"));
    }

    @Test
    public void tablesForTaskHandlesUnevenSplitAndEmptyShare() {
        List<TableId> all = List.of(
                new TableId(null, "public", "a"),
                new TableId(null, "public", "b"),
                new TableId(null, "public", "c"));
        assertThat(SmartSnapshotConnectorCoordinator.tablesForTask(all, 0, 2)).hasSize(2); // a, c
        assertThat(SmartSnapshotConnectorCoordinator.tablesForTask(all, 1, 2)).hasSize(1); // b
        // more tasks than tables -> the extra task gets nothing
        assertThat(SmartSnapshotConnectorCoordinator.tablesForTask(all, 5, 6)).isEmpty();
    }

    @Test
    public void parseTablesTrimsSkipsBlanksAndHandlesEmpty() {
        assertThat(SmartSnapshotConnectorCoordinator.parseTables(null)).isEmpty();
        assertThat(SmartSnapshotConnectorCoordinator.parseTables("")).isEmpty();
        assertThat(SmartSnapshotConnectorCoordinator.parseTables(" public.a , public.b ,")).containsExactly(
                new TableId(null, "public", "a"), new TableId(null, "public", "b"));
    }

    // ---- taskConfigs state machine --------------------------------------------------------------

    @Test
    public void taskConfigsActiveEmitsOneConfigPerTaskWithEpochAndCount() {
        List<Map<String, String>> configs = coordinator.taskConfigs(2, baseProps());

        assertThat(configs).hasSize(2);
        assertThat(configs.get(0)).containsEntry(ConfigurationNames.TASK_ID_PROPERTY_NAME, "0")
                .containsEntry(SnapshotCoordinationFacade.EPOCH, "0")
                .containsEntry(SnapshotCoordinationFacade.NUM_TASKS, "2");
        assertThat(configs.get(1)).containsEntry(ConfigurationNames.TASK_ID_PROPERTY_NAME, "1");
    }

    @Test
    public void restartTickBumpsEpochOnNextTaskConfigs() {
        coordinator.taskConfigs(2, baseProps()); // sets numTasks = 2, epoch = 0
        when(facade.isRestartNeeded("0", 0)).thenReturn(true);

        assertThat(coordinator.monitorTick()).isFalse();
        verify(connectorContext).requestTaskReconfiguration();

        List<Map<String, String>> next = coordinator.taskConfigs(2, baseProps());
        verify(facade).writeEpoch(1); // new epoch persisted before configs handed out
        assertThat(next.get(0)).containsEntry(SnapshotCoordinationFacade.EPOCH, "1");
    }

    @Test
    public void allTasksDoneTickCompletesAndTaskConfigsWritesCompletion() {
        coordinator.taskConfigs(2, baseProps()); // numTasks = 2, epoch = 0
        when(facade.isRestartNeeded(anyString(), eq(0))).thenReturn(false);
        when(facade.isDone("0", 0)).thenReturn(true);
        when(facade.isDone("1", 0)).thenReturn(true);
        when(facade.readSnapshotInfo()).thenReturn(Collect.hashMapOf(SnapshotCoordinationFacade.CONSISTENT_POINT, "0/16B3748"));

        assertThat(coordinator.monitorTick()).isTrue();
        verify(connectorContext).requestTaskReconfiguration();
        assertThat(coordinator.isComplete()).isTrue();

        assertThat(coordinator.taskConfigs(2, baseProps())).isNull(); // downscale
        verify(facade).writeCompletion("0/16B3748", 0);
    }

    @Test
    public void startSkipsWhenAStreamingOffsetAlreadyExists() {
        when(connectorContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        // offset present with no in-progress snapshot marker -> streaming already underway
        when(offsetStorageReader.offset(any())).thenReturn(Collect.hashMapOf("lsn", 123L));

        coordinator.start();

        assertThat(coordinator.isComplete()).isTrue();
        verify(facade, never()).start(); // returns before touching the coordination topic
    }

    @Test
    public void startSkipsWhenCoordinationTopicShowsCompleted() {
        when(connectorContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        when(facade.readSnapshotInfo()).thenReturn(Collect.hashMapOf(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY, true));

        coordinator.start();

        assertThat(coordinator.isComplete()).isTrue();
        verify(facade).start();
    }

    @Test
    public void startFreshReadsAndPersistsTheEpochThenRunsMonitor() {
        when(connectorContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        when(facade.readSnapshotInfo()).thenReturn(null);
        when(facade.readEpoch()).thenReturn(2);

        coordinator.start();

        assertThat(coordinator.isComplete()).isFalse();
        verify(facade, times(1)).writeEpoch(2); // persistEpoch(readEpoch())
    }

    @Test
    public void startOnVeryFirstDeploymentDefaultsEpochWhenNoneSaved() {
        // no offset, no snapshot info, and no saved epoch (brand-new coordination topic)
        when(connectorContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        when(facade.readSnapshotInfo()).thenReturn(null);
        when(facade.readEpoch()).thenReturn(null);

        // must not NPE on the null epoch; falls back to the initial epoch (0) and persists it
        coordinator.start();

        assertThat(coordinator.isComplete()).isFalse();
        verify(facade, times(1)).writeEpoch(0);
    }

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put("connector.class", "x");
        return props;
    }
}
