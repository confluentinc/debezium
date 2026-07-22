/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;

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

    // Snapshot ongoing: hand out the coordinator's per-task configs and keep the coordinator running.
    @Test
    public void taskConfigsReturnsCoordinatorConfigsWhileSnapshotting() {
        SmartSnapshotConnectorCoordinator coordinator = mock(SmartSnapshotConnectorCoordinator.class);
        List<Map<String, String>> perTask = List.of(new HashMap<>(smartProps()), new HashMap<>(smartProps()));
        when(coordinator.taskConfigs(2, smartProps())).thenReturn(perTask);
        when(coordinator.isComplete()).thenReturn(false);
        connector.initForTesting(smartProps(), coordinator);

        List<Map<String, String>> configs = connector.taskConfigs(2);

        assertThat(configs, is(perTask));
        assertThat(connector.smartSnapshotConnectorCoordinator(), is(coordinator)); // still running
        verify(coordinator, never()).stop();
    }

    // Snapshot complete: hand out the coordinator's single downscale config, then drop and stop the coordinator.
    @Test
    public void taskConfigsDownscalesAndStopsCoordinatorWhenComplete() {
        SmartSnapshotConnectorCoordinator coordinator = mock(SmartSnapshotConnectorCoordinator.class);
        List<Map<String, String>> single = Collections.singletonList(new HashMap<>(smartProps()));
        when(coordinator.taskConfigs(2, smartProps())).thenReturn(single);
        when(coordinator.isComplete()).thenReturn(true);
        connector.initForTesting(smartProps(), coordinator);

        List<Map<String, String>> configs = connector.taskConfigs(2);

        assertThat(configs, is(single));
        assertThat(connector.smartSnapshotConnectorCoordinator(), is(nullValue())); // dropped
        verify(coordinator).stop();
    }

    // maxTasks == 1: nothing to parallelize, so drop the coordinator and hand out a single config.
    @Test
    public void taskConfigsStopsCoordinatorAndHandsOutSingleConfigForOneTask() {
        SmartSnapshotConnectorCoordinator coordinator = mock(SmartSnapshotConnectorCoordinator.class);
        connector.initForTesting(smartProps(), coordinator);

        List<Map<String, String>> configs = connector.taskConfigs(1);

        assertThat(configs.size(), is(1));
        assertThat(configs.get(0), is(smartProps()));
        assertThat(connector.smartSnapshotConnectorCoordinator(), is(nullValue()));
        verify(coordinator).stop();
        verify(coordinator, never()).taskConfigs(anyInt(), any());
    }

    // Feature not applicable (no coordinator): hand out a single config.
    @Test
    public void taskConfigsHandsOutSingleConfigWhenNoCoordinator() {
        connector.initForTesting(smartProps(), null);

        List<Map<String, String>> configs = connector.taskConfigs(2);

        assertThat(configs.size(), is(1));
        assertThat(configs.get(0), is(smartProps()));
    }

    @Test
    public void taskConfigsReturnsEmptyWhenNotStarted() {
        connector.initForTesting(null, null);
        assertThat(connector.taskConfigs(2), is(Collections.emptyList()));
    }

    private static Map<String, String> smartProps() {
        return smartConfig(true, "initial").asMap();
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