/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

public class SqlServerPartition extends AbstractPartition implements Partition {
    private static final String SERVER_PARTITION_KEY = "server";
    private static final String DATABASE_PARTITION_KEY = "database";
    private static final String TASK_PARTITION_KEY = "task";

    private final String serverName;
    private final String taskId;
    private final Map<String, String> sourcePartition;
    private final List<Map<String, String>> supportedFormats;
    private final int hashCode;

    public SqlServerPartition(String serverName, String databaseName) {
        this(serverName, databaseName, true);
    }

    public SqlServerPartition(String serverName, String databaseName, boolean multiPartitionMode) {
        this(serverName, databaseName, multiPartitionMode, null);
    }

    /**
     * @param taskId non-null only for multi-task smart-snapshot sharded tasks; when set, the source
     *               partition becomes {server, database, task} so each shard tracks its own completion
     *               offset. Streaming (and every non-smart-snapshot path) keeps the legacy {server, database}.
     */
    public SqlServerPartition(String serverName, String databaseName, boolean multiPartitionMode, String taskId) {
        super(databaseName);
        this.serverName = serverName;
        this.taskId = taskId;

        this.sourcePartition = taskId != null
                ? Collect.hashMapOf(SERVER_PARTITION_KEY, serverName, DATABASE_PARTITION_KEY, databaseName, TASK_PARTITION_KEY, taskId)
                : Collect.hashMapOf(SERVER_PARTITION_KEY, serverName, DATABASE_PARTITION_KEY, databaseName);

        // for connectors working in single-partition mode the format of a partition has been changed in Debezium 2.0,
        // the legacy/old format of the partition should still be supported along with the new format
        // the new format has precedence over the old format
        this.supportedFormats = multiPartitionMode ? Collections.singletonList(this.sourcePartition)
                : Arrays.asList(this.sourcePartition, Collect.hashMapOf(SERVER_PARTITION_KEY, serverName));

        this.hashCode = Objects.hash(serverName, databaseName, taskId);
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    @Override
    public List<Map<String, String>> getSupportedFormats() {
        return supportedFormats;
    }

    /**
     * Returns the SQL Server database name corresponding to the partition.
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Returns the shared (per-database) source partition key {server, database} without the task id.
     * Used to read the streaming resume position / coordination data after downscale.
     */
    public Map<String, String> getSharedSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName, DATABASE_PARTITION_KEY, databaseName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SqlServerPartition other = (SqlServerPartition) obj;
        return Objects.equals(serverName, other.serverName) && Objects.equals(databaseName, other.databaseName)
                && Objects.equals(taskId, other.taskId);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "SqlServerPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<SqlServerPartition> {
        private final SqlServerConnectorConfig connectorConfig;

        Provider(SqlServerConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<SqlServerPartition> getPartitions() {
            String serverName = connectorConfig.getLogicalName();
            boolean multiPartitionMode = connectorConfig.getDatabaseNames().size() > 1;
            // Only smart-snapshot sharded tasks (which carry an epoch) get the task-scoped {server,database,task}
            // partition. Normal tasks also carry a task id but no epoch, so they keep the legacy {server,database}.
            String taskId = connectorConfig.getSmartSnapshotEpoch() != null ? connectorConfig.getTaskId() : null;

            return connectorConfig.getDatabaseNames().stream()
                    .map(databaseName -> new SqlServerPartition(serverName, databaseName, multiPartitionMode, taskId))
                    .collect(Collectors.toSet());
        }
    }
}
