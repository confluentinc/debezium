/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.junit.relational.TestRelationalDatabaseConfig;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Unit tests for {@link RelationalDatabaseConnectorConfig}, focusing on the
 * {@link RelationalDatabaseConnectorConfig#parseSnapshotSelectOverridesDataMap()} helper
 * and the {@link RelationalDatabaseConnectorConfig#getSnapshotSelectOverridesByTable()} flow
 * that consumes it.
 */
public class RelationalDatabaseConnectorConfigTest {

    private TestRelationalDatabaseConfig configWith(Configuration config) {
        return new TestRelationalDatabaseConfig(config, null, null, 0);
    }

    private TestRelationalDatabaseConfig configWithDataMap(String json) {
        return configWith(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP, json)
                .build());
    }

    // -------------------------------------------------------------------------
    // parseSnapshotSelectOverridesDataMap()
    // -------------------------------------------------------------------------

    @Test
    public void shouldReturnEmptyMapWhenDataMapConfigNotSet() {
        TestRelationalDatabaseConfig config = configWith(Configuration.create().build());

        Map<String, String> result = config.parseSnapshotSelectOverridesDataMap();

        assertThat(result).isEmpty();
    }

    @Test
    public void shouldParseValidJsonMapWithSingleTable() {
        TestRelationalDatabaseConfig config = configWithDataMap(
                "{\"db.table1\": \"SELECT * FROM db.table1 WHERE id > 100\"}");

        Map<String, String> result = config.parseSnapshotSelectOverridesDataMap();

        assertThat(result).hasSize(1);
        assertThat(result).containsEntry("db.table1", "SELECT * FROM db.table1 WHERE id > 100");
    }

    @Test
    public void shouldParseValidJsonMapWithMultipleTables() {
        TestRelationalDatabaseConfig config = configWithDataMap(
                "{\"db.table1\": \"SELECT * FROM db.table1 WHERE id > 100\", " +
                        "\"db.table2\": \"SELECT * FROM db.table2 WHERE ts > '2026-01-01'\"}");

        Map<String, String> result = config.parseSnapshotSelectOverridesDataMap();

        assertThat(result).hasSize(2);
        assertThat(result).containsEntry("db.table1", "SELECT * FROM db.table1 WHERE id > 100");
        assertThat(result).containsEntry("db.table2", "SELECT * FROM db.table2 WHERE ts > '2026-01-01'");
    }

    @Test
    public void shouldReturnEmptyMapWhenJsonIsEmptyObject() {
        TestRelationalDatabaseConfig config = configWithDataMap("{}");

        Map<String, String> result = config.parseSnapshotSelectOverridesDataMap();

        assertThat(result).isEmpty();
    }

    @Test
    public void shouldReturnEmptyMapAndLogWarningWhenJsonIsMalformed() {
        LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseConnectorConfig.class);
        TestRelationalDatabaseConfig config = configWithDataMap("{not valid json}");

        Map<String, String> result = config.parseSnapshotSelectOverridesDataMap();

        assertThat(result).isEmpty();
        assertThat(logInterceptor.containsWarnMessage(
                "Failed to parse snapshot.select.statement.overrides.data.map as JSON")).isTrue();
    }

    @Test
    public void shouldHandleSpecialCharactersInSelectStatement() {
        TestRelationalDatabaseConfig config = configWithDataMap(
                "{\"dbo.users\": \"SELECT * FROM [dbo].[users] WHERE last_name = 'O''Brien'\"}");

        Map<String, String> result = config.parseSnapshotSelectOverridesDataMap();

        assertThat(result).hasSize(1);
        assertThat(result).containsEntry("dbo.users",
                "SELECT * FROM [dbo].[users] WHERE last_name = 'O''Brien'");
    }

    // -------------------------------------------------------------------------
    // getSnapshotSelectOverridesByTable() — integration of the helper
    // -------------------------------------------------------------------------

    @Test
    public void shouldReturnEmptyOverridesWhenNeitherConfigSet() {
        TestRelationalDatabaseConfig config = configWith(Configuration.create().build());

        Map<DataCollectionId, String> result = config.getSnapshotSelectOverridesByTable();

        assertThat(result).isEmpty();
    }

    @Test
    public void shouldBuildOverridesFromDataMap() {
        TestRelationalDatabaseConfig config = configWith(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "db.table1")
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP,
                        "{\"db.table1\": \"SELECT * FROM db.table1 WHERE id > 100\"}")
                .build());

        Map<DataCollectionId, String> result = config.getSnapshotSelectOverridesByTable();

        assertThat(result).hasSize(1);
        assertThat(result).containsEntry(TableId.parse("db.table1"),
                "SELECT * FROM db.table1 WHERE id > 100");
    }

    @Test
    public void shouldBuildOverridesFromDataMapForMultipleTables() {
        TestRelationalDatabaseConfig config = configWith(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "db.t1,db.t2")
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP,
                        "{\"db.t1\": \"SELECT * FROM db.t1 WHERE pk > 101\", " +
                                "\"db.t2\": \"SELECT * FROM db.t2 WHERE pk > 100\"}")
                .build());

        Map<DataCollectionId, String> result = config.getSnapshotSelectOverridesByTable();

        assertThat(result).hasSize(2);
        assertThat(result).containsEntry(TableId.parse("db.t1"), "SELECT * FROM db.t1 WHERE pk > 101");
        assertThat(result).containsEntry(TableId.parse("db.t2"), "SELECT * FROM db.t2 WHERE pk > 100");
    }

    @Test
    public void shouldSkipTableWhenNotInDataMap() {
        LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseConnectorConfig.class);
        TestRelationalDatabaseConfig config = configWith(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "db.table1,db.missing")
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP,
                        "{\"db.table1\": \"SELECT * FROM db.table1 WHERE id > 100\"}")
                .build());

        Map<DataCollectionId, String> result = config.getSnapshotSelectOverridesByTable();

        assertThat(result).hasSize(1);
        assertThat(result).containsEntry(TableId.parse("db.table1"),
                "SELECT * FROM db.table1 WHERE id > 100");
        assertThat(logInterceptor.containsWarnMessage(
                "Detected snapshot.select.statement.overrides for snapshot.select.statement.overrides.db.missing but no statement property db.missing defined")).isTrue();
    }
}
