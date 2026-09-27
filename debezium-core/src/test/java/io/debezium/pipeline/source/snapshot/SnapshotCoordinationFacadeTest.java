/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.DebeziumException;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

/**
 * Unit tests for {@link SnapshotCoordinationFacade}: the typed key/value layer over the coordination store.
 * A mock {@link SnapshotCoordination} stands in for Kafka, so these assert the record shape and the
 * epoch-aware flag reads without a broker.
 */
public class SnapshotCoordinationFacadeTest {

    private static final String SERVER = "srv";

    @Mock
    private SnapshotCoordination coordination;

    private SnapshotCoordinationFacade facade;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);
        facade = new SnapshotCoordinationFacade(coordination, SERVER);
    }

    @Test
    public void writeEpochUsesTheEpochKey() throws Exception {
        facade.writeEpoch(3);

        verify(coordination).write(Collect.hashMapOf("server", SERVER, "type", "epoch_marker"),
                Collect.hashMapOf("epoch", 3));
    }

    @Test
    public void readEpochReturnsTheStoredValue() {
        when(coordination.read(Collect.hashMapOf("server", SERVER, "type", "epoch_marker")))
                .thenReturn(Collect.hashMapOf("epoch", 7));

        assertThat(facade.readEpoch()).isEqualTo(7);
    }

    @Test
    public void readEpochReturnsNullWhenAbsent() {
        when(coordination.read(any())).thenReturn(null);

        assertThat(facade.readEpoch()).isNull();
    }

    @Test
    public void isTaskDoneRequiresMatchingEpoch() {
        Map<String, String> key = Collect.hashMapOf("server", SERVER, "task", "2", "type", "task_done");
        when(coordination.read(key)).thenReturn(Collect.hashMapOf("epoch", 5));

        assertThat(facade.isTaskDone("2", 5)).isTrue();
        assertThat(facade.isTaskDone("2", 4)).isFalse(); // epoch mismatch -> stale
    }

    @Test
    public void isDoneIsTaskFalseWhenMissingOrExistsAtEpochUnset() {
        Map<String, String> key = Collect.hashMapOf("server", SERVER, "task", "2", "type", "task_done");
        when(coordination.read(key)).thenReturn(null);
        assertThat(facade.isTaskDone("2", 5)).isFalse();
    }

    @Test
    public void isRestartNeededRequiresMatchingEpoch() {
        Map<String, String> key = Collect.hashMapOf("server", SERVER, "task", "0", "type", "task_restart");
        when(coordination.read(key)).thenReturn(Collect.hashMapOf("epoch", 1));

        assertThat(facade.isRestartNeeded("0", 1)).isTrue();
        assertThat(facade.isRestartNeeded("0", 2)).isFalse();
    }

    @Test
    public void isTaskStartedTransactionRequiresMatchingEpoch() {
        Map<String, String> key = Collect.hashMapOf("server", SERVER, "task", "1", "type", "task_started_transaction");
        when(coordination.read(key)).thenReturn(Collect.hashMapOf("epoch", 9));

        assertThat(facade.isTaskStartedTransaction("1", 9)).isTrue();
        assertThat(facade.isTaskStartedTransaction("1", 8)).isFalse();
    }

    @Test
    public void allTasksJoinedCatchesUpOnceThenReadsEveryTaskFromTheLocalView() {
        for (int i = 0; i < 3; i++) {
            when(coordination.readCached(taskKey(i, "task_join"))).thenReturn(Collect.hashMapOf("epoch", 4));
        }

        assertThat(facade.allTasksJoined(3, 4)).isTrue();

        // one catch-up for the whole check, no synchronous per-task read
        verify(coordination, times(1)).catchUp();
        verify(coordination, times(3)).readCached(any());
        verify(coordination, never()).read(any());
    }

    @Test
    public void allTasksChecksRequireEveryTaskAtTheMatchingEpoch() {
        when(coordination.readCached(taskKey(0, "task_started_transaction"))).thenReturn(Collect.hashMapOf("epoch", 4));
        when(coordination.readCached(taskKey(1, "task_started_transaction"))).thenReturn(Collect.hashMapOf("epoch", 3)); // stale
        when(coordination.readCached(taskKey(0, "task_done"))).thenReturn(Collect.hashMapOf("epoch", 4));
        // task-1 has no done marker at all

        assertThat(facade.allTasksStartedTransaction(2, 4)).isFalse();
        assertThat(facade.allTasksStartedTransaction(1, 4)).isTrue();
        assertThat(facade.allTasksDone(2, 4)).isFalse();
    }

    @Test
    public void anyRestartNeededMatchesOnlyTheGivenEpochAndCatchesUpOnce() {
        when(coordination.readCached(taskKey(1, "task_restart"))).thenReturn(Collect.hashMapOf("epoch", 2));

        assertThat(facade.anyRestartNeeded(3, 2)).isTrue();
        assertThat(facade.anyRestartNeeded(3, 3)).isFalse(); // the marker is from another round

        verify(coordination, times(2)).catchUp();
        verify(coordination, never()).read(any());
    }

    @Test
    public void allTasksCheckPropagatesACatchUpFailure() {
        doThrow(new DebeziumException("broker down")).when(coordination).catchUp();

        // the poll loops treat a DebeziumException as a transient failure and retry
        assertThatThrownBy(() -> facade.allTasksJoined(2, 1)).isInstanceOf(DebeziumException.class);
        verify(coordination, never()).readCached(any());
    }

    private static Map<String, String> taskKey(int taskId, String type) {
        return Collect.hashMapOf("server", SERVER, "task", String.valueOf(taskId), "type", type);
    }

    @Test
    public void writeSnapshotInfoStoresNameLsnAssignmentsAndTaskCount() throws Exception {
        List<TableId> tables = List.of(new TableId(null, "public", "a"), new TableId(null, "public", "b"));

        facade.writeSnapshotInfo("snap", "0/16B3748", 99L, 4, tables, 2);

        ArgumentCaptor<Map<String, Object>> value = valueCaptor();
        verify(coordination).write(eq(Collect.hashMapOf("server", SERVER, "type", "snapshot_info")), value.capture());
        assertThat(value.getValue()).containsEntry("snapshot_name", "snap")
                .containsEntry("consistent_point", "0/16B3748")
                .containsEntry("txid", 99L)
                .containsEntry("epoch", 4)
                .containsEntry("num_tasks", 2)
                // explicit per-task slice: task-0 -> [a], task-1 -> [b] after the stable sort + round-robin
                .containsEntry("assignments", SmartSnapshotTableAssignments.buildAssignments(tables, 2));
    }

    @Test
    public void writeSnapshotInfoOmitsTxidWhenNull() throws Exception {
        List<TableId> tables = List.of(new TableId(null, "public", "a"));

        facade.writeSnapshotInfo("snap", "0/16B3748", null, 4, tables, 1);

        ArgumentCaptor<Map<String, Object>> value = valueCaptor();
        verify(coordination).write(eq(Collect.hashMapOf("server", SERVER, "type", "snapshot_info")), value.capture());
        assertThat(value.getValue()).doesNotContainKey("txid");
    }

    @Test
    public void writeCompletionMarksSnapshotCompleted() throws Exception {
        facade.writeCompletion("0/16B3748", 4);

        ArgumentCaptor<Map<String, Object>> value = valueCaptor();
        verify(coordination).write(eq(Collect.hashMapOf("server", SERVER, "type", "snapshot_done")), value.capture());
        assertThat(value.getValue())
                .containsEntry("consistent_point", "0/16B3748")
                .containsEntry("epoch", 4);
    }

    @Test
    public void readTaskJoinEpochReturnsTheStoredEpoch() {
        when(coordination.read(Collect.hashMapOf("server", SERVER, "task", "0", "type", "task_join")))
                .thenReturn(Collect.hashMapOf("epoch", 6));

        assertThat(facade.readTaskJoinEpoch("0")).isEqualTo(6);
    }

    @Test
    public void writeJoinUsesThePerTaskJoinKey() throws Exception {
        facade.writeTaskJoin("0", 6);

        verify(coordination).write(Collect.hashMapOf("server", SERVER, "task", "0", "type", "task_join"),
                Collect.hashMapOf("epoch", 6));
    }

    @Test
    public void writeTaskDoneMarksTheTaskCompleted() throws Exception {
        facade.writeTaskDone("1", 3);

        verify(coordination).write(Collect.hashMapOf("server", SERVER, "task", "1", "type", "task_done"),
                Collect.hashMapOf("epoch", 3));
    }

    @Test
    public void writeRestartNeededSetsTheRestartExistsAtEpoch() throws Exception {
        facade.writeRestartNeeded("2", 8);

        verify(coordination).write(Collect.hashMapOf("server", SERVER, "task", "2", "type", "task_restart"),
                Collect.hashMapOf("epoch", 8));
    }

    @Test
    public void writeTaskStartedTransaction() throws Exception {
        facade.writeTaskStartedTransaction("0", 5);

        verify(coordination).write(Collect.hashMapOf("server", SERVER, "task", "0", "type", "task_started_transaction"),
                Collect.hashMapOf("epoch", 5));
    }

    @Test
    public void readSnapshotInfoReadsTheSnapshotInfoKey() {
        when(coordination.read(Collect.hashMapOf("server", SERVER, "type", "snapshot_info"))).thenReturn(Collect.hashMapOf("snapshot_name", "snap"));

        assertThat(facade.readSnapshotInfo()).containsEntry("snapshot_name", "snap");
    }

    @Test
    public void readSnapshotInfoForEpochReturnsOnlyAPublishedRecordForThatEpoch() {
        Map<String, String> key = Collect.hashMapOf("server", SERVER, "type", "snapshot_info");
        when(coordination.read(key)).thenReturn(Collect.hashMapOf("consistent_point", "0/16B3748", "epoch", 5));

        assertThat(facade.readSnapshotInfo(5)).containsEntry("consistent_point", "0/16B3748");
        // the record is keyed by server only, so the latest one may belong to another round
        assertThat(facade.readSnapshotInfo(6)).isNull();
    }

    @Test
    public void readSnapshotInfoForEpochIsNullUntilTheConsistentPointIsPublished() {
        Map<String, String> key = Collect.hashMapOf("server", SERVER, "type", "snapshot_info");

        when(coordination.read(key)).thenReturn(null);
        assertThat(facade.readSnapshotInfo(5)).isNull();

        // readiness keys on the consistent point, not on the snapshot name (MySQL publishes none)
        when(coordination.read(key)).thenReturn(Collect.hashMapOf("snapshot_name", "snap", "epoch", 5));
        assertThat(facade.readSnapshotInfo(5)).isNull();
    }

    @Test
    public void epochOfIsNullSafeAndCoercesNumbers() {
        assertThat(SnapshotCoordinationFacade.epochOf(null)).isNull();
        assertThat(SnapshotCoordinationFacade.epochOf(Collect.hashMapOf("x", 1))).isNull();
        assertThat(SnapshotCoordinationFacade.epochOf(Collect.hashMapOf("epoch", 3L))).isEqualTo(3);
    }

    @Test
    public void writeFailuresAreWrappedInDebeziumException() throws Exception {
        doThrow(new RuntimeException("topic down")).when(coordination).write(any(), any());

        assertThatThrownBy(() -> facade.writeEpoch(1)).isInstanceOf(DebeziumException.class);
    }

    @SuppressWarnings("unchecked")
    private static ArgumentCaptor<Map<String, Object>> valueCaptor() {
        return ArgumentCaptor.forClass(Map.class);
    }
}
