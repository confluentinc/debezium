/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.pipeline.source.snapshot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

public class SnapshotCoordinationFacade {

    // A record on the coordination topic can carry a "type" so two records for the same server
    // don't overwrite each other.
    private static final String TYPE = "type";
    private static final String TYPE_SNAPSHOT_INFO = "snapshot_info";
    private static final String TYPE_EPOCH = "epoch";
    private static final String TYPE_STARTED = "started";
    private static final String TYPE_RESTART = "restart";
    private static final String TYPE_JOIN = "join";
    private static final String TYPE_DONE = "done";

    public static final String EPOCH = "epoch";
    public static final String SNAPSHOT_NAME = "snapshot_name";
    public static final String CONSISTENT_POINT = "consistent_point";
    public static final String TABLES = "tables";
    public static final String NUM_TASKS = "num_tasks";
    public static final String SNAPSHOT_COMPLETED = "snapshot_completed";
    public static final String TRANSACTION_STARTED = "transaction_started";
    public static final String RESTART_NEEDED = "restart_needed";
    public static final String COMPLETED = "completed";

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

    private Map<String, String> epochKey() {
        return Collect.hashMapOf("server", server, TYPE, TYPE_EPOCH);
    }

    private Map<String, String> startedKey(String taskId) {
        return Collect.hashMapOf("server", server, "task", taskId, TYPE, TYPE_STARTED);
    }

    private Map<String, String> restartKey(String taskId) {
        return Collect.hashMapOf("server", server, "task", taskId, TYPE, TYPE_RESTART);
    }

    private Map<String, String> joinKey(String taskId) {
        return Collect.hashMapOf("server", server, "task", taskId, TYPE, TYPE_JOIN);
    }

    private Map<String, String> doneKey(String taskId) {
        return Collect.hashMapOf("server", server, "task", taskId, TYPE, TYPE_DONE);
    }

    public void writeSnapshotInfo(String snapshotName, String consistentPoint, int epoch, List<TableId> tables, int numTasks) {
        Map<String, Object> value = new HashMap<>();
        value.put(SNAPSHOT_NAME, snapshotName);
        value.put(CONSISTENT_POINT, consistentPoint);
        value.put(EPOCH, epoch);
        value.put(SNAPSHOT_COMPLETED, false);
        value.put(TABLES, tables.stream().map(TableId::toString).collect(Collectors.joining(",")));
        value.put(NUM_TASKS, numTasks);
        write(snapshotInfoKey(), value);
    }

    public void writeCompletion(String consistentPoint, int epoch) {
        Map<String, Object> value = new HashMap<>();
        value.put(CONSISTENT_POINT, consistentPoint);
        value.put(EPOCH, epoch);
        value.put(SNAPSHOT_COMPLETED, true);
        write(snapshotInfoKey(), value);
    }

    public Map<String, Object> readSnapshotInfo() {
        return coordination.read(snapshotInfoKey());
    }

    public void writeEpoch(int epoch) {
        write(epochKey(), Collect.hashMapOf(EPOCH, epoch));
    }

    public Integer readEpoch() {
        return epochOf(coordination.read(epochKey()));
    }

    public void writeTransactionStarted(String taskId, int epoch) {
        write(startedKey(taskId), Collect.hashMapOf(TRANSACTION_STARTED, true, EPOCH, epoch));
    }

    public boolean isTransactionStarted(String taskId, int epoch) {
        return flag(startedKey(taskId), TRANSACTION_STARTED, epoch);
    }

    public void writeRestartNeeded(String taskId, int epoch) {
        write(restartKey(taskId), Collect.hashMapOf(RESTART_NEEDED, true, EPOCH, epoch));
    }

    public boolean isRestartNeeded(String taskId, int epoch) {
        return flag(restartKey(taskId), RESTART_NEEDED, epoch);
    }

    public void writeJoin(String taskId, int epoch) {
        write(joinKey(taskId), Collect.hashMapOf(EPOCH, epoch));
    }

    public Integer readJoinEpoch(String taskId) {
        return epochOf(coordination.read(joinKey(taskId)));
    }

    public void writeDone(String taskId, int epoch) {
        write(doneKey(taskId), Collect.hashMapOf(COMPLETED, true, EPOCH, epoch));
    }

    public boolean isDone(String taskId, int epoch) {
        return flag(doneKey(taskId), COMPLETED, epoch);
    }

    public static Integer epochOf(Map<String, Object> value) {
        return (value != null &&
                value.get(EPOCH) != null) ? ((Number) value.get(EPOCH)).intValue() : null;
    }

    private boolean flag(Map<String, String> key, String field, int epoch) {
        Map<String, Object> value = coordination.read(key);
        Integer epochOfValue = epochOf(value);
        return value != null &&
                Boolean.TRUE.equals(value.get(field)) &&
                epochOfValue != null &&
                epochOfValue == epoch;
    }

    private void write(Map<String, String> key, Map<String, Object> value) {
        try {
            // throws -> callers that want best-effort catch it
            coordination.write(key, value);
        }
        catch (Exception e) {
            throw new DebeziumException("Smart snapshot: Coordination write failed for " + key, e);
        }
    }
}
