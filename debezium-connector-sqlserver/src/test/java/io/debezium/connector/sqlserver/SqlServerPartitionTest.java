/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

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
    public void legacyPartitionHasNoTaskKeyComponent() {
        SqlServerPartition partition = new SqlServerPartition("server1", "database1", true, null);
        assertThat(partition.getSourcePartition()).containsOnlyKeys("server", "database");
    }

    @Test
    public void shardedSmartSnapshotPartitionCarriesTaskKeyComponent() {
        SqlServerPartition partition = new SqlServerPartition("server1", "database1", true, "0");
        assertThat(partition.getSourcePartition()).containsOnly(
                entry("server", "server1"),
                entry("database", "database1"),
                entry("task", "0"));
    }

    @Test
    public void shardedPartitionsForSameDatabaseWithDifferentTaskIndexAreNotEqual() {
        SqlServerPartition shard0 = new SqlServerPartition("server1", "database1", true, "0");
        SqlServerPartition shard1 = new SqlServerPartition("server1", "database1", true, "1");

        assertThat(shard0).isNotEqualTo(shard1);
        assertThat(shard0.hashCode()).isNotEqualTo(shard1.hashCode());
    }

    @Test
    public void legacyAndShardedPartitionsForSameDatabaseAreNotEqual() {
        SqlServerPartition legacy = new SqlServerPartition("server1", "database1", true, null);
        SqlServerPartition sharded = new SqlServerPartition("server1", "database1", true, "0");

        assertThat(legacy).isNotEqualTo(sharded);
    }
}
