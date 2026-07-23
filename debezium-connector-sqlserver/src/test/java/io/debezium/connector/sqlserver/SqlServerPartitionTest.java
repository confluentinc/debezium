/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

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
    public void sourcePartitionWithoutTaskIdOmitsTaskKey() {
        Map<String, String> source = new SqlServerPartition("server1", "database1", false, null).getSourcePartition();
        assertThat(source).containsEntry("server", "server1").containsEntry("database", "database1");
        assertThat(source).doesNotContainKey("task");
    }

    @Test
    public void sourcePartitionWithTaskIdIncludesTaskKey() {
        Map<String, String> source = new SqlServerPartition("server1", "database1", false, "0").getSourcePartition();
        assertThat(source).containsEntry("server", "server1").containsEntry("database", "database1").containsEntry("task", "0");
    }

    // Smart-snapshot correctness relies on this distinction: a sharded task keys its data offset by task id, but
    // schema history must be written task-agnostically so every consumer (siblings + the downscaled streaming
    // task) recovers it under the same {server,database} source. Guard the two sources against accidentally
    // converging.
    @Test
    public void taskScopedSourceDiffersFromTaskAgnosticSource() {
        Map<String, String> taskAgnostic = new SqlServerPartition("server1", "database1", false, null).getSourcePartition();
        Map<String, String> taskScoped = new SqlServerPartition("server1", "database1", false, "0").getSourcePartition();
        assertThat(taskScoped).isNotEqualTo(taskAgnostic);
    }

    @Test
    public void differentTaskIdsProduceDistinctSources() {
        Map<String, String> task0 = new SqlServerPartition("server1", "database1", false, "0").getSourcePartition();
        Map<String, String> task1 = new SqlServerPartition("server1", "database1", false, "1").getSourcePartition();
        assertThat(task0).isNotEqualTo(task1);
    }
}
