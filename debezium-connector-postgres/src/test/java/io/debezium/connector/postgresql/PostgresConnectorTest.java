/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;

public class PostgresConnectorTest {
    PostgresConnector connector;

    @Before
    public void before() {
        connector = new PostgresConnector();
    }

    @Test
    public void testValidateUnableToConnectNoThrow() {
        Map<String, String> config = new HashMap<>();
        config.put(PostgresConnectorConfig.HOSTNAME.name(), "narnia");
        config.put(PostgresConnectorConfig.PORT.name(), "1234");
        config.put(PostgresConnectorConfig.DATABASE_NAME.name(), "postgres");
        config.put(PostgresConnectorConfig.USER.name(), "pikachu");
        config.put(PostgresConnectorConfig.PASSWORD.name(), "pika");
        config.put(PostgresConnectorConfig.TOPIC_PREFIX.name(), "topic-prefix");

        Config validated = connector.validate(config);
        for (ConfigValue value : validated.configValues()) {
            if (config.containsKey(value.name())
                    && !value.name().equals(PostgresConnectorConfig.TOPIC_PREFIX.name())) {
                assertThat(value.errorMessages().get(0), is("Error while validating connector config: The connection attempt failed."));
            }
        }
    }

    // Smart snapshot engages only for the data-copying modes; other modes fall back to the single-task path
    // (guards the "always double-snapshots / no_data wasteful" regression).
    @Test
    public void smartSnapshotAppliesForDataSnapshotModes() {
        assertThat(PostgresConnector.smartSnapshotApplies(smartConfig(true, "initial")), is(true));
        assertThat(PostgresConnector.smartSnapshotApplies(smartConfig(true, "initial_only")), is(true));
        assertThat(PostgresConnector.smartSnapshotApplies(smartConfig(true, "when_needed")), is(true));
    }

    @Test
    public void smartSnapshotDoesNotApplyForNonDataModes() {
        assertThat(PostgresConnector.smartSnapshotApplies(smartConfig(true, "always")), is(false));
        assertThat(PostgresConnector.smartSnapshotApplies(smartConfig(true, "never")), is(false));
        assertThat(PostgresConnector.smartSnapshotApplies(smartConfig(true, "no_data")), is(false));
    }

    @Test
    public void smartSnapshotDoesNotApplyWhenDisabled() {
        assertThat(PostgresConnector.smartSnapshotApplies(smartConfig(false, "initial")), is(false));
    }

    private static Configuration smartConfig(boolean enabled, String snapshotMode) {
        return Configuration.create()
                .with(PostgresConnectorConfig.HOSTNAME, "localhost")
                .with(PostgresConnectorConfig.PORT, 5432)
                .with(PostgresConnectorConfig.USER, "user")
                .with(PostgresConnectorConfig.PASSWORD, "pass")
                .with(PostgresConnectorConfig.DATABASE_NAME, "db")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "srv")
                .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, enabled)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, snapshotMode)
                .build();
    }
}