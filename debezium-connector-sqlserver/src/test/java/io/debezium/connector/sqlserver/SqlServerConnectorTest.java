/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Connector;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;

public class SqlServerConnectorTest {
    SqlServerConnector connector;

    @Before
    public void before() {
        connector = new SqlServerConnector();
    }

    @Test
    public void testValidateUnableToConnectNoThrow() {
        Map<String, String> config = new HashMap<>();
        config.put(CommonConnectorConfig.TOPIC_PREFIX.name(), "dbserver1");
        config.put(SqlServerConnectorConfig.HOSTNAME.name(), "narnia");
        config.put(SqlServerConnectorConfig.PORT.name(), "4321");
        config.put(SqlServerConnectorConfig.DATABASE_NAMES.name(), "sqlserver");
        config.put(SqlServerConnectorConfig.USER.name(), "pikachu");
        config.put(SqlServerConnectorConfig.PASSWORD.name(), "raichu");

        Config validated = connector.validate(config);
        ConfigValue hostName = getHostName(validated).orElseThrow(() -> new IllegalArgumentException("Host name config option not found"));
        assertThat(hostName.errorMessages().get(0).startsWith("Unable to connect:"));
    }

    private Optional<ConfigValue> getHostName(Config config) {
        return config.configValues()
                .stream()
                .filter(value -> value.name().equals(SqlServerConnectorConfig.HOSTNAME.name()))
                .findFirst();
    }

    @Test
    public void shouldReturnConfigurationDefinition() {
        assertConfigDefIsValid(connector, SqlServerConnectorConfig.ALL_FIELDS);
    }

    private Configuration smartConfig(boolean enabled, String snapshotMode, String... databaseNames) {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                .with(SqlServerConnectorConfig.DATABASE_NAMES, String.join(",", databaseNames))
                .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, enabled)
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, snapshotMode)
                .build();
    }

    @Test
    public void smartSnapshotAppliesForDataSnapshotModes() {
        assertThat(SqlServerConnector.smartSnapshotApplies(smartConfig(true, "initial", "db1"))).isTrue();
        assertThat(SqlServerConnector.smartSnapshotApplies(smartConfig(true, "initial_only", "db1"))).isTrue();
        assertThat(SqlServerConnector.smartSnapshotApplies(smartConfig(true, "when_needed", "db1"))).isTrue();
    }

    @Test
    public void smartSnapshotDoesNotApplyForNonDataModes() {
        assertThat(SqlServerConnector.smartSnapshotApplies(smartConfig(true, "no_data", "db1"))).isFalse();
        assertThat(SqlServerConnector.smartSnapshotApplies(smartConfig(true, "schema_only", "db1"))).isFalse();
    }

    @Test
    public void smartSnapshotDoesNotApplyWhenDisabled() {
        assertThat(SqlServerConnector.smartSnapshotApplies(smartConfig(false, "initial", "db1"))).isFalse();
    }

    // Phase 0: smart snapshot is restricted to single-database connectors.
    @Test
    public void smartSnapshotDoesNotApplyForMultiDatabaseConnectors() {
        assertThat(SqlServerConnector.smartSnapshotApplies(smartConfig(true, "initial", "db1", "db2"))).isFalse();
    }

    // Phase 0: repeatable_read (default) and read_committed engage; snapshot/read_uncommitted/exclusive fall
    // back to single-task (read_uncommitted is unsafe for the L_db + CDC-catch-up model, snapshot/exclusive
    // are not validated for the single-anchor model).
    @Test
    public void smartSnapshotAppliesUnderRepeatableReadAndReadCommitted() {
        assertThat(SqlServerConnector.smartSnapshotApplies(
                smartConfig(true, "initial", "db1").edit().with(SqlServerConnectorConfig.SNAPSHOT_ISOLATION_MODE, "repeatable_read").build())).isTrue();
        assertThat(SqlServerConnector.smartSnapshotApplies(
                smartConfig(true, "initial", "db1").edit().with(SqlServerConnectorConfig.SNAPSHOT_ISOLATION_MODE, "read_committed").build())).isTrue();
        assertThat(SqlServerConnector.smartSnapshotApplies(
                smartConfig(true, "initial", "db1").edit().with(SqlServerConnectorConfig.SNAPSHOT_ISOLATION_MODE, "snapshot").build())).isFalse();
        assertThat(SqlServerConnector.smartSnapshotApplies(
                smartConfig(true, "initial", "db1").edit().with(SqlServerConnectorConfig.SNAPSHOT_ISOLATION_MODE, "read_uncommitted").build())).isFalse();
    }

    protected static void assertConfigDefIsValid(Connector connector, io.debezium.config.Field.Set fields) {
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
        fields.forEach(expected -> {
            assertThat(configDef.names()).contains(expected.name());
            ConfigKey key = configDef.configKeys().get(expected.name());
            assertThat(key).isNotNull();
            assertThat(key.name).isEqualTo(expected.name());
            assertThat(key.displayName).isEqualTo(expected.displayName());
            assertThat(key.importance).isEqualTo(expected.importance());
            assertThat(key.documentation).isEqualTo(expected.description());
            assertThat(key.type).isEqualTo(expected.type());
            if (expected.equals(SqlServerConnectorConfig.SCHEMA_HISTORY) || expected.equals(CommonConnectorConfig.TOPIC_NAMING_STRATEGY)) {
                assertThat(((Class<?>) key.defaultValue).getName()).isEqualTo((String) expected.defaultValue());
            }
            else if (expected.type() == ConfigDef.Type.LIST && key.defaultValue != null) {
                assertThat(key.defaultValue).isEqualTo(Arrays.asList(expected.defaultValue()));
            }
            assertThat(key.dependents).isEqualTo(expected.dependents());
            assertThat(key.width).isNotNull();
            assertThat(key.group).isNotNull();
            assertThat(key.orderInGroup).isGreaterThan(0);
            if ((key.validator != null)) {
                assertThat(key.validator)
                        .withFailMessage("Validator should be instance of ConfigDef.Validator for field: %s", expected.name())
                        .isInstanceOf(ConfigDef.Validator.class);
            }
            else {
                assertThat(key.validator).isNull();
            }
            assertThat(key.recommender).isNull();
        });
    }
}
