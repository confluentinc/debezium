/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.debezium.relational.TableId;

/**
 * Unit tests for {@link SmartSnapshotTableAssignments}: the pure table-to-task split (serialize/parse, stable sort +
 * round-robin, per-task slice lookup) with no coordination-topic I/O.
 */
public class SmartSnapshotTableAssignmentsTest {

    @Test
    public void parseTablesTrimsSkipsBlanksAndHandlesEmpty() {
        assertThat(SmartSnapshotTableAssignments.parseTables(null, false)).isEmpty();
        assertThat(SmartSnapshotTableAssignments.parseTables("", false)).isEmpty();
        List<TableId> tables = List.of(
                new TableId(null, "public", "a"),
                new TableId(null, "public", "b"));
        assertThat(SmartSnapshotTableAssignments.parseTables(SmartSnapshotTableAssignments.joinTableIds(tables), false)).containsExactly(
                tables.get(0), tables.get(1));
    }

    @Test
    public void parseTablesInterpretsTwoPartAsCatalogWhenUseCatalogScoped() {
        // MySQL-style identifiers are catalog.table (no schema). With useCatalogScoped=true a 2-part FQN must be
        // read back as catalog.table, not schema.table.
        List<TableId> tables = List.of(
                new TableId("db1", null, "a"),
                new TableId("db1", null, "b"));
        List<TableId> parsed = SmartSnapshotTableAssignments.parseTables(SmartSnapshotTableAssignments.joinTableIds(tables), true);

        assertThat(parsed).containsExactly(tables.get(0), tables.get(1));
        assertThat(parsed.get(0).catalog()).isEqualTo("db1");
        assertThat(parsed.get(0).schema()).isNull();
        assertThat(parsed.get(0).table()).isEqualTo("a");
    }

    @Test
    public void testPostgresDialectSchemaParsingIdentity() {
        // REMEDIATION: Set catalog to null to match Debezium's native Postgres TableId generation
        TableId originalTable = new TableId(null, "chaos,schema", "customers,export");
        List<TableId> discoveredTables = List.of(originalTable);

        // This now generates a 2-part string: "\"chaos,schema\".\"customers,export\""
        Object serializedPayload = SmartSnapshotTableAssignments.joinTableIds(discoveredTables);

        // Run through the parsing plane
        List<TableId> parsedTables = SmartSnapshotTableAssignments.parseTables(serializedPayload, false);

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
        Object serializedJsonArray = SmartSnapshotTableAssignments.joinTableIds(discoveredTables);

        assertTrue(serializedJsonArray instanceof List);

        @SuppressWarnings("unchecked")
        List<String> jsonStringArray = (List<String>) serializedJsonArray;

        // Verify the exact double-quoted format generated
        // Debezium's toDoubleQuotedString output format: "catalog"."schema"."table"
        assertEquals("\"chaos,schema\".\"customers,export\"", jsonStringArray.get(0));
        assertEquals("\"chaos,schema\".\"data.metrics.v1\"", jsonStringArray.get(1));
        assertEquals("\"chaos,schema\".\"report\"\"internal\"\"final\"", jsonStringArray.get(2)); // Double quotes escaped internally

        // 3. Test Deserialization: Parse it back using the TableIdParser hook
        List<TableId> parsedTables = SmartSnapshotTableAssignments.parseTables(serializedJsonArray, false);

        assertEquals(discoveredTables.size(), parsedTables.size());

        // Assert every single table recovered its exact structural identity
        assertEquals(tableWithComma, parsedTables.get(0));
        assertEquals(tableWithDot, parsedTables.get(1));
        assertEquals(tableWithQuotes, parsedTables.get(2));
        assertEquals(tableWithSpaces, parsedTables.get(3));

        // 4. Test Shard Routing (tablesForTask Stride Loop)
        // Ensure that the odd naming characters do not throw off the round-robin allocation
        List<TableId> task0Shard = SmartSnapshotTableAssignments.tablesForTask(parsedTables, 0, 2);
        List<TableId> task1Shard = SmartSnapshotTableAssignments.tablesForTask(parsedTables, 1, 2);

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
        assertThat(SmartSnapshotTableAssignments.tablesForTask(all, 0, 2))
                .containsExactly(new TableId(null, "public", "a"), new TableId(null, "public", "c"));
        assertThat(SmartSnapshotTableAssignments.tablesForTask(all, 1, 2))
                .containsExactly(new TableId(null, "public", "b"), new TableId(null, "public", "d"));
    }

    @Test
    public void tablesForTaskHandlesUnevenSplitAndEmptyShare() {
        List<TableId> all = List.of(
                new TableId(null, "public", "a"),
                new TableId(null, "public", "b"),
                new TableId(null, "public", "c"));
        assertThat(SmartSnapshotTableAssignments.tablesForTask(all, 0, 2)).hasSize(2); // a, c
        assertThat(SmartSnapshotTableAssignments.tablesForTask(all, 1, 2)).hasSize(1); // b
        // more tasks than tables -> the extra task gets nothing
        assertThat(SmartSnapshotTableAssignments.tablesForTask(all, 5, 6)).isEmpty();
    }

    @Test
    public void assignmentForTaskReturnsTheTaskSliceAndEmptyForMissing() {
        List<TableId> tables = List.of(new TableId(null, "public", "a"), new TableId(null, "public", "b"));
        Map<String, Object> assignments = SmartSnapshotTableAssignments.buildAssignments(tables, 2);

        assertThat(SmartSnapshotTableAssignments.parseTables(SmartSnapshotTableAssignments.assignmentForTask(assignments, 0), false))
                .containsExactly(new TableId(null, "public", "a"));
        assertThat(SmartSnapshotTableAssignments.parseTables(SmartSnapshotTableAssignments.assignmentForTask(assignments, 1), false))
                .containsExactly(new TableId(null, "public", "b"));
        // a task with no published slice (e.g. more tasks than tables) gets an empty subset, not an error
        assertThat(SmartSnapshotTableAssignments.assignmentForTask(assignments, 5)).isEqualTo(List.of());
        assertThat(SmartSnapshotTableAssignments.assignmentForTask(null, 0)).isEqualTo(List.of());
    }
}
