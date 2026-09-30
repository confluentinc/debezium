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
    private static final String TASKS_MAX = "tasks.max";
    private static final String PRODUCER_BOOTSTRAP = "producer.override.bootstrap.servers";

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
        // This test exercises connection validation only; keep it independent of the smart-snapshot feature default.
        config.put(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED.name(), "false");

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

    // --- Smart snapshot config validation (RelationalBaseSourceConnector#validateSmartSnapshotConfig +
    // PostgresConnector#validateSmartSnapshotMode) ---

    // Unsupported snapshot mode with the feature on: rejected with a user-facing error on snapshot.mode.
    @Test
    public void validateRejectsUnsupportedSnapshotModeWhenSmartSnapshotEnabled() {
        Config validated = connector.validate(validateProps(true, "always", 2, true));
        assertThat(hasSmartSnapshotError(validated, PostgresConnectorConfig.SNAPSHOT_MODE.name()), is(true));
    }

    // Feature on but only one task: nothing to parallelize, rejected on tasks.max.
    @Test
    public void validateRejectsSingleTaskWhenSmartSnapshotEnabled() {
        Config validated = connector.validate(validateProps(true, "initial", 1, true));
        assertThat(hasSmartSnapshotError(validated, TASKS_MAX), is(true));
    }

    // Feature on but no coordination bootstrap: tasks cannot coordinate, rejected on the bootstrap override.
    @Test
    public void validateRejectsMissingCoordinationBootstrapWhenSmartSnapshotEnabled() {
        Config validated = connector.validate(validateProps(true, "initial", 2, false));
        assertThat(hasSmartSnapshotError(validated, PRODUCER_BOOTSTRAP), is(true));
    }

    // All prerequisites wrong at once: every offending key is reported (no short-circuiting between checks).
    @Test
    public void validateReportsAllSmartSnapshotPrerequisiteErrors() {
        Config validated = connector.validate(validateProps(true, "always", 1, false));
        assertThat(hasSmartSnapshotError(validated, PostgresConnectorConfig.SNAPSHOT_MODE.name()), is(true));
        assertThat(hasSmartSnapshotError(validated, TASKS_MAX), is(true));
        assertThat(hasSmartSnapshotError(validated, PRODUCER_BOOTSTRAP), is(true));
    }

    // Well-formed smart snapshot config: no prerequisite errors (any remaining errors are just the unreachable DB).
    @Test
    public void validateAcceptsWellFormedSmartSnapshotConfig() {
        Config validated = connector.validate(validateProps(true, "initial", 2, true));
        assertThat(hasSmartSnapshotError(validated, PostgresConnectorConfig.SNAPSHOT_MODE.name()), is(false));
        assertThat(hasSmartSnapshotError(validated, TASKS_MAX), is(false));
        assertThat(hasSmartSnapshotError(validated, PRODUCER_BOOTSTRAP), is(false));
    }

    // Feature off: an otherwise-incompatible config (always mode, single task, no bootstrap) is left alone.
    @Test
    public void validateSkipsSmartSnapshotChecksWhenDisabled() {
        Config validated = connector.validate(validateProps(false, "always", 1, false));
        assertThat(hasSmartSnapshotError(validated, PostgresConnectorConfig.SNAPSHOT_MODE.name()), is(false));
        assertThat(hasSmartSnapshotError(validated, TASKS_MAX), is(false));
        assertThat(hasSmartSnapshotError(validated, PRODUCER_BOOTSTRAP), is(false));
    }

    private static Map<String, String> validateProps(boolean enabled, String snapshotMode, int tasksMax, boolean withBootstrap) {
        Map<String, String> props = new HashMap<>();
        // Unresolvable host keeps the test hermetic: connection validation fails fast without a real database.
        props.put(PostgresConnectorConfig.HOSTNAME.name(), "narnia");
        props.put(PostgresConnectorConfig.PORT.name(), "1234");
        props.put(PostgresConnectorConfig.DATABASE_NAME.name(), "postgres");
        props.put(PostgresConnectorConfig.USER.name(), "user");
        props.put(PostgresConnectorConfig.PASSWORD.name(), "pass");
        props.put(CommonConnectorConfig.TOPIC_PREFIX.name(), "srv");
        props.put(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED.name(), String.valueOf(enabled));
        props.put(PostgresConnectorConfig.SNAPSHOT_MODE.name(), snapshotMode);
        props.put(TASKS_MAX, String.valueOf(tasksMax));
        if (withBootstrap) {
            props.put(PRODUCER_BOOTSTRAP, "localhost:9092");
        }
        return props;
    }

    private static boolean hasSmartSnapshotError(Config validated, String key) {
        return validated.configValues().stream()
                .filter(value -> value.name().equals(key))
                .flatMap(value -> value.errorMessages().stream())
                .anyMatch(message -> message.contains("Smart snapshot"));
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