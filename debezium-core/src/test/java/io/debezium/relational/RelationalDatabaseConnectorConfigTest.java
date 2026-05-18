/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
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
    public void shouldThrowWhenJsonIsMalformed() {
        TestRelationalDatabaseConfig config = configWithDataMap("{not valid json}");

        assertThatThrownBy(() -> config.parseSnapshotSelectOverridesDataMap())
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Failed to parse 'snapshot.select.statement.overrides.data.map' as JSON");
    }

    @Test
    public void validatorAcceptsValidJson() {
        Configuration config = Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP,
                        "{\"db.table1\": \"SELECT * FROM db.table1\"}")
                .build();

        Map<String, ConfigValue> validated = config.validate(
                Field.setOf(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP));

        assertThat(validated.get(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP.name())
                .errorMessages()).isEmpty();
    }

    @Test
    public void validatorRejectsMalformedJson() {
        Configuration config = Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP, "{not valid json}")
                .build();

        Map<String, ConfigValue> validated = config.validate(
                Field.setOf(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP));

        List<String> errors = validated.get(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP.name())
                .errorMessages();
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("It's not a valid JSON");
    }

    @Test
    public void validatorRejectsWhenBothByTableAndDataMapAreSet() {
        Configuration config = Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "db.t1")
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name() + ".db.t1",
                        "SELECT * FROM db.t1")
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP,
                        "{\"db.t1\": \"SELECT * FROM db.t1\"}")
                .build();

        Map<String, ConfigValue> validated = config.validate(
                Field.setOf(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP));

        List<String> errors = validated.get(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP.name())
                .errorMessages();
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("Cannot be set together with 'snapshot.select.statement.overrides'");
    }

    @Test
    public void validatorAcceptsUnsetConfig() {
        Configuration config = Configuration.create().build();

        Map<String, ConfigValue> validated = config.validate(
                Field.setOf(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP));

        assertThat(validated.get(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_DATA_MAP.name())
                .errorMessages()).isEmpty();
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
        TestRelationalDatabaseConfig config = configWithDataMap(
                "{\"db.table1\": \"SELECT * FROM db.table1 WHERE id > 100\"}");

        Map<DataCollectionId, String> result = config.getSnapshotSelectOverridesByTable();

        assertThat(result).hasSize(1);
        assertThat(result).containsEntry(TableId.parse("db.table1"),
                "SELECT * FROM db.table1 WHERE id > 100");
    }

    @Test
    public void shouldBuildOverridesFromDataMapForMultipleTables() {
        TestRelationalDatabaseConfig config = configWithDataMap(
                "{\"db.t1\": \"SELECT * FROM db.t1 WHERE pk > 101\", " +
                        "\"db.t2\": \"SELECT * FROM db.t2 WHERE pk > 100\"}");

        Map<DataCollectionId, String> result = config.getSnapshotSelectOverridesByTable();

        assertThat(result).hasSize(2);
        assertThat(result).containsEntry(TableId.parse("db.t1"), "SELECT * FROM db.t1 WHERE pk > 101");
        assertThat(result).containsEntry(TableId.parse("db.t2"), "SELECT * FROM db.t2 WHERE pk > 100");
    }

    @Test
    public void shouldBuildOverridesFromLegacyByTableAndDynamicConfigs() {
        TestRelationalDatabaseConfig config = configWith(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "db.table1")
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name() + ".db.table1",
                        "SELECT * FROM db.table1 WHERE id > 100")
                .build());

        Map<DataCollectionId, String> result = config.getSnapshotSelectOverridesByTable();

        assertThat(result).hasSize(1);
        assertThat(result).containsEntry(TableId.parse("db.table1"),
                "SELECT * FROM db.table1 WHERE id > 100");
    }

    @Test
    public void shouldBuildOverridesFromLegacyByTableForMultipleTables() {
        TestRelationalDatabaseConfig config = configWith(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "db.t1,db.t2")
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name() + ".db.t1",
                        "SELECT * FROM db.t1 WHERE pk > 101")
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name() + ".db.t2",
                        "SELECT * FROM db.t2 WHERE pk > 100")
                .build());

        Map<DataCollectionId, String> result = config.getSnapshotSelectOverridesByTable();

        assertThat(result).hasSize(2);
        assertThat(result).containsEntry(TableId.parse("db.t1"), "SELECT * FROM db.t1 WHERE pk > 101");
        assertThat(result).containsEntry(TableId.parse("db.t2"), "SELECT * FROM db.t2 WHERE pk > 100");
    }

    @Test
    public void shouldSkipTableInLegacyPathWhenDynamicConfigMissing() {
        LogInterceptor logInterceptor = new LogInterceptor(RelationalDatabaseConnectorConfig.class);
        TestRelationalDatabaseConfig config = configWith(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, "db.table1,db.missing")
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name() + ".db.table1",
                        "SELECT * FROM db.table1 WHERE id > 100")
                .build());

        Map<DataCollectionId, String> result = config.getSnapshotSelectOverridesByTable();

        assertThat(result).hasSize(1);
        assertThat(result).containsEntry(TableId.parse("db.table1"),
                "SELECT * FROM db.table1 WHERE id > 100");
        assertThat(logInterceptor.containsWarnMessage(
                "Detected snapshot.select.statement.overrides for snapshot.select.statement.overrides.db.missing but no statement property db.missing defined")).isTrue();
    }

}
