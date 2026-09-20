/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.config.ConfigurationNames.TASK_ID_PROPERTY_NAME;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.DATABASE_NAME;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

public class PostgresPartition extends AbstractPartition implements Partition {
    static final String TASK_PARTITION_KEY = "task";
    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;
    private final String taskId;

    public PostgresPartition(String serverName, String databaseName) {
        this(serverName, databaseName, null);
    }

    public PostgresPartition(String serverName, String databaseName, String taskId) {
        super(databaseName);
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
     * Returns the shared source partition key (without task ID).
     * Used by the leader task to write the streaming resume offset
     * and by all tasks to read coordination data.
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
        final PostgresPartition other = (PostgresPartition) obj;
        return Objects.equals(serverName, other.serverName) && Objects.equals(taskId, other.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverName, taskId);
    }

    @Override
    public String toString() {
        return "PostgresPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<PostgresPartition> {
        private final PostgresConnectorConfig connectorConfig;
        private final Configuration taskConfig;

        Provider(PostgresConnectorConfig connectorConfig, Configuration taskConfig) {
            this.connectorConfig = connectorConfig;
            this.taskConfig = taskConfig;
        }

        @Override
        public Set<PostgresPartition> getPartitions() {
            // taskId is null in streaming or smart.snapshot disabled case
            String taskId = taskConfig.getString(TASK_ID_PROPERTY_NAME);
            return Collections.singleton(
                    new PostgresPartition(
                            connectorConfig.getLogicalName(),
                            taskConfig.getString(DATABASE_NAME.name()),
                            taskId));
        }
    }
}
