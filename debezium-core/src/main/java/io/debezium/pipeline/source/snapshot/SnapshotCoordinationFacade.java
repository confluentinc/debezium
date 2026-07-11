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
import java.util.function.Function;
import java.util.stream.Collectors;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

public class SnapshotCoordinationFacade {

    // A record on the coordination topic can carry a "type" so two records for the same server
    // don't overwrite each other.
    private static final String TYPE = "type";
    private static final String TYPE_SNAPSHOT_INFO = "snapshot_info";
    // server-level completion
    private static final String TYPE_SNAPSHOT_DONE = "snapshot_done";
    private static final String TYPE_EPOCH = "epoch_marker";
    private static final String TYPE_TASK_STARTED_TRANSACTION = "task_started_transaction";
    private static final String TYPE_TASK_RESTART = "task_restart";
    private static final String TYPE_TASK_JOIN = "task_join";
    private static final String TYPE_TASK_DONE = "task_done";

    public static final String EPOCH = "epoch";
    public static final String SNAPSHOT_NAME = "snapshot_name";
    public static final String CONSISTENT_POINT = "consistent_point";
    // Explicit per-task table assignment: taskId (as a string) -> JSON array of quoted table FQNs.
    // The leader publishes the whole plan in this single field so each task reads its own slice directly
    // instead of re-deriving it from a flat table list.
    public static final String ASSIGNMENTS = "assignments";
    public static final String NUM_TASKS = "num_tasks";

    private final SnapshotCoordination coordination;
    private final String server;

    public SnapshotCoordinationFacade(Configuration configuration, CommonConnectorConfig connectorConfig) {
        this(new KafkaLogSnapshotCoordination(configuration, connectorConfig), connectorConfig.getLogicalName());
    }

    // visible for testing: inject a coordination store (a fake / mock) without touching Kafka.
    SnapshotCoordinationFacade(SnapshotCoordination coordination, String server) {
        this.coordination = coordination;
        this.server = server;
    }

    public static boolean hasCoordinationBootstrap(Configuration config, CommonConnectorConfig connectorConfig) {
        return KafkaLogSnapshotCoordination.hasBootstrap(config, connectorConfig);
    }

    /**
     * Read-only facade that does NOT create the coordination topic.
     */
    public static SnapshotCoordinationFacade readOnly(Configuration config, CommonConnectorConfig connectorConfig) {
        return new SnapshotCoordinationFacade(
                new KafkaLogSnapshotCoordination(config, connectorConfig, false),
                connectorConfig.getLogicalName());
    }

    public boolean startForRead() {
        return coordination.startForRead();
    }

    public void start() {
        coordination.start();
    }

    public void stop() {
        coordination.stop();
    }

    private Map<String, String> snapshotInfoKey() {
        return Collect.hashMapOf("server", server, TYPE, TYPE_SNAPSHOT_INFO);
    }

    private Map<String, String> snapshotDoneKey() {
        return Collect.hashMapOf("server", server, TYPE, TYPE_SNAPSHOT_DONE);
    }

    private Map<String, String> epochKey() {
        return Collect.hashMapOf("server", server, TYPE, TYPE_EPOCH);
    }

    private Map<String, String> taskStartedTransactionKey(String taskId) {
        return Collect.hashMapOf("server", server, "task", taskId, TYPE, TYPE_TASK_STARTED_TRANSACTION);
    }

    private Map<String, String> taskRestartKey(String taskId) {
        return Collect.hashMapOf("server", server, "task", taskId, TYPE, TYPE_TASK_RESTART);
    }

    private Map<String, String> taskJoinKey(String taskId) {
        return Collect.hashMapOf("server", server, "task", taskId, TYPE, TYPE_TASK_JOIN);
    }

    private Map<String, String> taskDoneKey(String taskId) {
        return Collect.hashMapOf("server", server, "task", taskId, TYPE, TYPE_TASK_DONE);
    }

    public void writeSnapshotInfo(String snapshotName, String consistentPoint, int epoch, List<TableId> tables, int numTasks) {
        Map<String, Object> value = new HashMap<>();
        value.put(SNAPSHOT_NAME, snapshotName);
        value.put(CONSISTENT_POINT, consistentPoint);
        value.put(EPOCH, epoch);
        value.put(NUM_TASKS, numTasks);
        // Publish the explicit per-task slice rather than a flat table list.
        value.put(ASSIGNMENTS, buildAssignments(tables, numTasks));
        write(snapshotInfoKey(), value);
    }

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
     * Pull one task's slice out of the published ASSIGNMENTS map. Returns the raw JSON array of quoted FQNs
     * (empty if the task has no entry) — feed it to {@link #parseTablesPostgres(Object)} to get TableIds.
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
     * Decode the TABLES field (a JSON array of quoted FQNs from the snapshot-info record) back to TableIds.
     */
    @SuppressWarnings("unchecked")
    public static List<TableId> parseTablesPostgres(Object tablesValue) {
        if (!(tablesValue instanceof List)) {
            return List.of();
        }
        return ((List<String>) tablesValue).stream()
                .filter(s -> s != null && !s.isBlank())
                // Pass false to interpret 2-part identifiers as schema.table (Postgres style)
                .map(s -> TableId.parse(s, false))
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

    public void writeCompletion(String consistentPoint, int epoch) {
        Map<String, Object> value = new HashMap<>();
        value.put(CONSISTENT_POINT, consistentPoint);
        value.put(EPOCH, epoch);
        write(snapshotDoneKey(), value);
    }

    // no epoch check
    public Map<String, Object> readSnapshotInfo() {
        return coordination.read(snapshotInfoKey());
    }

    public Map<String, Object> readCompletion() {
        // has consistent_point + epoch; non-null = done
        return coordination.read(snapshotDoneKey());
    }

    public void writeEpoch(int epoch) {
        write(epochKey(), Collect.hashMapOf(EPOCH, epoch));
    }

    public Integer readEpoch() {
        return epochOf(coordination.read(epochKey()));
    }

    public void writeTaskStartedTransaction(String taskId, int epoch) {
        write(taskStartedTransactionKey(taskId), Collect.hashMapOf(EPOCH, epoch));
    }

    public boolean isTaskStartedTransaction(String taskId, int epoch) {
        return existsAtEpoch(taskStartedTransactionKey(taskId), epoch);
    }

    public void writeRestartNeeded(String taskId, int epoch) {
        write(taskRestartKey(taskId), Collect.hashMapOf(EPOCH, epoch));
    }

    public boolean isRestartNeeded(String taskId, int epoch) {
        return existsAtEpoch(taskRestartKey(taskId), epoch);
    }

    public void writeTaskJoin(String taskId, int epoch) {
        write(taskJoinKey(taskId), Collect.hashMapOf(EPOCH, epoch));
    }

    public Integer readTaskJoinEpoch(String taskId) {
        return epochOf(coordination.read(taskJoinKey(taskId)));
    }

    public boolean isTaskJoined(String taskId, int epoch) {
        return existsAtEpoch(taskJoinKey(taskId), epoch);
    }

    public void writeTaskDone(String taskId, int epoch) {
        write(taskDoneKey(taskId), Collect.hashMapOf(EPOCH, epoch));
    }

    public boolean isTaskDone(String taskId, int epoch) {
        return existsAtEpoch(taskDoneKey(taskId), epoch);
    }

    public static Integer epochOf(Map<String, Object> value) {
        return (value != null &&
                value.get(EPOCH) != null) ? ((Number) value.get(EPOCH)).intValue() : null;
    }

    public static <O extends OffsetContext> O fetchOffsetFromCoordinationTopic(
            Configuration config, CommonConnectorConfig connectorConfig, boolean isSmartSnapshotTask,
            Function<String, O> buildOffsetFromConsistentPoint) {
        // Post-downscale streaming task: read consistent point from coordination topic only if the feature is still enabled
        // Otherwise, the snapshot taken in the smart snapshot mode is discarded
        if (!connectorConfig.isSmartSnapshotEnabled() || isSmartSnapshotTask) {
            return null;
        }

        if (!SnapshotCoordinationFacade.hasCoordinationBootstrap(config, connectorConfig)) {
            return null;
        }

        SnapshotCoordinationFacade facade = SnapshotCoordinationFacade.readOnly(config, connectorConfig);

        try {
            if (!facade.startForRead()) {
                // topic doesn't exist / broker unreachable skip fast
                return null;
            }

            Map<String, Object> completionInfo = facade.readCompletion();

            if (completionInfo != null
                    && completionInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT) != null) {
                String cp = String.valueOf(completionInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT));
                return buildOffsetFromConsistentPoint.apply(cp);
            }
            return null;
        }
        finally {
            facade.stop();
        }
    }

    private boolean existsAtEpoch(Map<String, String> key, int epoch) {
        Map<String, Object> value = coordination.read(key);
        Integer epochOfValue = epochOf(value);
        return value != null &&
                epochOfValue != null &&
                epochOfValue == epoch;
    }

    private void write(Map<String, String> key, Map<String, Object> value) {
        try {
            // throws -> callers that want best-effort catch it
            coordination.write(key, value);
        }
        catch (Exception e) {
            throw new DebeziumException("Smart snapshot: [role=coordination] Coordination write failed for " + key, e);
        }
    }
}
