/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.connector.common.AbstractPartitionTest;

public class SqlServerPartitionTest extends AbstractPartitionTest<SqlServerPartition> {

    @Override
    protected SqlServerPartition createPartition1() {
        return new SqlServerPartition("server1", "database1");
    }

    @Override
    protected SqlServerPartition createPartition2() {
        return new SqlServerPartition("server2", "database2");
    }

    @Test
    public void nonSmartSnapshotPartitionUsesLegacyKey() {
        // No task id -> legacy {server, database} key (unchanged for existing/streaming connectors).
        SqlServerPartition partition = new SqlServerPartition("server1", "database1", true);
        assertThat(partition.getSourcePartition()).containsOnlyKeys("server", "database");
        assertThat(partition.getSourcePartition()).containsEntry("server", "server1").containsEntry("database", "database1");
    }

    @Test
    public void smartSnapshotShardedPartitionAddsTaskKey() {
        // A sharded smart-snapshot task tracks its own completion under {server, database, task}.
        SqlServerPartition partition = new SqlServerPartition("server1", "database1", true, "2");
        assertThat(partition.getSourcePartition()).containsOnlyKeys("server", "database", "task");
        assertThat(partition.getSourcePartition()).containsEntry("task", "2");
        // The shared key (used for streaming resume / coordination) never carries the task id.
        assertThat(partition.getSharedSourcePartition()).containsOnlyKeys("server", "database");
    }

    @Test
    public void taskIdDistinguishesPartitions() {
        SqlServerPartition task0 = new SqlServerPartition("server1", "database1", true, "0");
        SqlServerPartition task1 = new SqlServerPartition("server1", "database1", true, "1");
        SqlServerPartition noTask = new SqlServerPartition("server1", "database1", true);
        assertThat(task0).isNotEqualTo(task1);
        assertThat(task0).isNotEqualTo(noTask);
        assertThat(task0).isEqualTo(new SqlServerPartition("server1", "database1", true, "0"));
    }
}
