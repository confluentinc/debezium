/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.relational.TableId;

/**
 * table-to-task assignment logic for a smart snapshot: the leader splits the discovered tables across
 * tasks (stable sort + round-robin) and publishes the plan; each task reads its own slice back. This is deliberately
 * separate from {@link SnapshotCoordinationFacade}, whose job is coordination-topic I/O rather than assignment math.
 */
public final class SmartSnapshotTableAssignments {

    /**
     * Build the explicit taskId -> slice map. Each slice is a JSON array of quoted FQNs. The round-robin split
     * runs here (on the leader) so it is computed once and published, not re-derived on every task.
     */
    public static Map<String, Object> buildAssignments(List<TableId> tables, int numTasks) {
        Map<String, Object> assignments = new HashMap<>();
        for (int i = 0; i < numTasks; i++) {
            assignments.put(String.valueOf(i), joinTableIds(tablesForTask(tables, i, numTasks)));
        }
        return assignments;
    }

    /**
     * Pull one task's slice out of the published assignments map. Returns the raw JSON array of quoted FQNs
     * (empty if the task has no entry) — feed it to {@link #parseTables(Object, boolean)} to get TableIds.
     */
    @SuppressWarnings("unchecked")
    public static Object assignmentForTask(Object assignmentsValue, int taskId) {
        if (!(assignmentsValue instanceof Map)) {
            return List.of();
        }
        Object taskTableAssignment = ((Map<String, Object>) assignmentsValue).get(String.valueOf(taskId));
        return taskTableAssignment != null ? taskTableAssignment : List.of();
    }

    public static Object joinTableIds(List<TableId> tables) {
        // JSON array of quoted FQNs — survives names with commas/dots (comma-join + split(",") is unsafe)
        return tables.stream().map(TableId::toDoubleQuotedString).collect(Collectors.toList());
    }

    /**
     * Decode a task's table assignment (a JSON array of quoted FQNs from the snapshot-info record) back to
     * TableIds. {@code useCatalogScoped} controls how a 2-part identifier is interpreted: pass {@code false}
     * for schema.table (Postgres style) and {@code true} for catalog.table (MySQL style).
     */
    @SuppressWarnings("unchecked")
    public static List<TableId> parseTables(Object tablesValue, boolean useCatalogScoped) {
        if (!(tablesValue instanceof List)) {
            return List.of();
        }
        return ((List<String>) tablesValue).stream()
                .filter(s -> s != null && !s.isBlank())
                .map(s -> TableId.parse(s, useCatalogScoped))
                .collect(Collectors.toList());
    }

    /**
     * Deterministic per-task subset: stable sort by name, round-robin by task id.
     */
    public static List<TableId> tablesForTask(List<TableId> allTables, int taskId, int numTasks) {
        List<TableId> sorted = new ArrayList<>(allTables);
        sorted.sort(Comparator.comparing(TableId::toString));
        List<TableId> mine = new ArrayList<>();
        for (int i = taskId; i < sorted.size(); i += numTasks) { // i = taskId, taskId+numTasks, ...
            mine.add(sorted.get(i));
        }
        return mine;
    }
}
