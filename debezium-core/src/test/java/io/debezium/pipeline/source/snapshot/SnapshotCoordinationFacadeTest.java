/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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
    public void parseTablesTrimsSkipsBlanksAndHandlesEmpty() {
        assertThat(SnapshotCoordinationFacade.parseTablesPostgres(null)).isEmpty();
        assertThat(SnapshotCoordinationFacade.parseTablesPostgres("")).isEmpty();
        List<TableId> tables = List.of(
                new TableId(null, "public", "a"),
                new TableId(null, "public", "b"));
        assertThat(SnapshotCoordinationFacade.parseTablesPostgres(SnapshotCoordinationFacade.joinTableIds(tables))).containsExactly(
                tables.get(0), tables.get(1));
    }

    @Test
    public void testPostgresDialectSchemaParsingIdentity() {
        // REMEDIATION: Set catalog to null to match Debezium's native Postgres TableId generation
        TableId originalTable = new TableId(null, "chaos,schema", "customers,export");
        List<TableId> discoveredTables = List.of(originalTable);

        // This now generates a 2-part string: "\"chaos,schema\".\"customers,export\""
        Object serializedPayload = SnapshotCoordinationFacade.joinTableIds(discoveredTables);

        // Run through the parsing plane
        List<TableId> parsedTables = SnapshotCoordinationFacade.parseTablesPostgres(serializedPayload);

        assertEquals(1, parsedTables.size());
        TableId resultTable = parsedTables.get(0);

        // This assertion will now FAIL on the old code (yielding null)
        // and PASS only with TableId.parse(s, false)
        assertEquals("chaos,schema", resultTable.schema());

        assertEquals("customers,export", resultTable.table());
    }

    @Test
    public void testAdversarialTableNamesSerializationAndRouting() {
        TableId tableWithComma = new TableId("chaos,schema", null, "customers,export");
        TableId tableWithDot = new TableId("chaos,schema", null, "data.metrics.v1");
        TableId tableWithQuotes = new TableId("chaos,schema", null, "report\"internal\"final");
        TableId tableWithSpaces = new TableId("chaos,schema", null, "$Weird Spaces & Caps#");

        List<TableId> discoveredTables = List.of(
                tableWithComma,
                tableWithDot,
                tableWithQuotes,
                tableWithSpaces);

        // Convert to double-quoted string list (what goes into Jackson)
        Object serializedJsonArray = SnapshotCoordinationFacade.joinTableIds(discoveredTables);

        assertTrue(serializedJsonArray instanceof List);

        @SuppressWarnings("unchecked")
        List<String> jsonStringArray = (List<String>) serializedJsonArray;

        // Verify the exact double-quoted format generated
        // Debezium's toDoubleQuotedString output format: "catalog"."schema"."table"
        assertEquals("\"chaos,schema\".\"customers,export\"", jsonStringArray.get(0));
        assertEquals("\"chaos,schema\".\"data.metrics.v1\"", jsonStringArray.get(1));
        assertEquals("\"chaos,schema\".\"report\"\"internal\"\"final\"", jsonStringArray.get(2)); // Double quotes escaped internally

        // 3. Test Deserialization: Parse it back using the TableIdParser hook
        List<TableId> parsedTables = SnapshotCoordinationFacade.parseTablesPostgres(serializedJsonArray);

        assertEquals(discoveredTables.size(), parsedTables.size());

        // Assert every single table recovered its exact structural identity
        assertEquals(tableWithComma, parsedTables.get(0));
        assertEquals(tableWithDot, parsedTables.get(1));
        assertEquals(tableWithQuotes, parsedTables.get(2));
        assertEquals(tableWithSpaces, parsedTables.get(3));

        // 4. Test Shard Routing (tablesForTask Stride Loop)
        // Ensure that the odd naming characters do not throw off the round-robin allocation
        List<TableId> task0Shard = SnapshotCoordinationFacade.tablesForTask(parsedTables, 0, 2);
        List<TableId> task1Shard = SnapshotCoordinationFacade.tablesForTask(parsedTables, 1, 2);

        assertEquals(2, task0Shard.size());
        assertEquals(2, task1Shard.size());

        // Verify that the tables are cleanly divided without mixing fragments or throwing string allocation errors
        assertTrue(task0Shard.contains(tableWithComma) || task0Shard.contains(tableWithDot));
        assertTrue(task1Shard.contains(tableWithQuotes) || task1Shard.contains(tableWithSpaces));
    }

    @Test
    public void tablesForTaskSplitsRoundRobinAfterStableSort() {
        List<TableId> all = List.of(
                new TableId(null, "public", "d"),
                new TableId(null, "public", "b"),
                new TableId(null, "public", "a"),
                new TableId(null, "public", "c"));
        // sorted: a, b, c, d
        assertThat(SnapshotCoordinationFacade.tablesForTask(all, 0, 2))
                .containsExactly(new TableId(null, "public", "a"), new TableId(null, "public", "c"));
        assertThat(SnapshotCoordinationFacade.tablesForTask(all, 1, 2))
                .containsExactly(new TableId(null, "public", "b"), new TableId(null, "public", "d"));
    }

    @Test
    public void tablesForTaskHandlesUnevenSplitAndEmptyShare() {
        List<TableId> all = List.of(
                new TableId(null, "public", "a"),
                new TableId(null, "public", "b"),
                new TableId(null, "public", "c"));
        assertThat(SnapshotCoordinationFacade.tablesForTask(all, 0, 2)).hasSize(2); // a, c
        assertThat(SnapshotCoordinationFacade.tablesForTask(all, 1, 2)).hasSize(1); // b
        // more tasks than tables -> the extra task gets nothing
        assertThat(SnapshotCoordinationFacade.tablesForTask(all, 5, 6)).isEmpty();
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
    public void writeSnapshotInfoStoresNameLsnTablesAndTaskCount() throws Exception {
        List<TableId> tables = List.of(new TableId(null, "public", "a"), new TableId(null, "public", "b"));

        facade.writeSnapshotInfo("snap", "0/16B3748", 4, tables, 2);

        ArgumentCaptor<Map<String, Object>> value = valueCaptor();
        verify(coordination).write(eq(Collect.hashMapOf("server", SERVER, "type", "snapshot_info")), value.capture());
        assertThat(value.getValue()).containsEntry("snapshot_name", "snap")
                .containsEntry("consistent_point", "0/16B3748")
                .containsEntry("epoch", 4)
                .containsEntry("num_tasks", 2)
                .containsEntry("tables", SnapshotCoordinationFacade.joinTableIds(tables));
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
