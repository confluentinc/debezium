/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.config.ConfigurationNames.TASK_ID_PROPERTY_NAME;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.DATABASE_NAME;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogPartition;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

public class MySqlPartition extends BinlogPartition {

    private static final String SERVER_PARTITION_KEY = "server";
    static final String TASK_PARTITION_KEY = "task";

    private final String serverName;
    private final String taskId;

    public MySqlPartition(String serverName, String databaseName) {
        this(serverName, databaseName, null);
    }

    public MySqlPartition(String serverName, String databaseName, String taskId) {
        super(serverName, databaseName);
        this.serverName = serverName;
        this.taskId = taskId;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        if (taskId != null) {
            return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName, TASK_PARTITION_KEY, taskId);
        }
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    /**
     * Shared source partition key (without task id) — used to read coordination data and, after downscale, the
     * streaming resume offset.
     */
    public Map<String, String> getSharedSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final MySqlPartition other = (MySqlPartition) obj;
        return Objects.equals(serverName, other.serverName) && Objects.equals(taskId, other.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverName, taskId);
    }

    @Override
    public String toString() {
        return "MySqlPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    public static class Provider implements Partition.Provider<MySqlPartition> {
        private final MySqlConnectorConfig connectorConfig;
        private final Configuration taskConfig;

        public Provider(MySqlConnectorConfig connectorConfig, Configuration taskConfig) {
            this.connectorConfig = connectorConfig;
            this.taskConfig = taskConfig;
        }

        @Override
        public Set<MySqlPartition> getPartitions() {
            // taskId is null when streaming or when smart.snapshot is disabled
            String taskId = taskConfig.getString(TASK_ID_PROPERTY_NAME);
            return Collections.singleton(new MySqlPartition(
                    connectorConfig.getLogicalName(),
                    taskConfig.getString(DATABASE_NAME.name()),
                    taskId));
        }
    }
}
