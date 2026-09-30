/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.ArrayList;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.util.Strings;

/**
 * Base class for Debezium's relational CDC {@link BaseSourceConnector} implementations. Provides functionality common to
 * all relational CDC connectors, such as validation.
 */
public abstract class RelationalBaseSourceConnector extends BaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalBaseSourceConnector.class);
    private static final String SERVER_ID = "database.server.id";
    private static final String TASKS_MAX_CONFIG = "tasks.max";
    private static final String PRODUCER_BOOTSTRAP_OVERRIDE = "producer.override.bootstrap.servers";

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Configuration config = Configuration.from(connectorConfigs);

        // Validate all the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = validateAllFields(config);

        // Smart (multi-task) snapshot prerequisites. Added before the connection check below so a misconfiguration
        // fails fast with a user-facing message and does not even open a database connection.
        validateSmartSnapshotConfig(config, results);

        if (Strings.isNullOrEmpty(config.getString(RelationalDatabaseConnectorConfig.PASSWORD))) {
            LOGGER.info("The connection password is empty");
        }

        results.values().stream()
                .filter(configValue -> !configValue.errorMessages().isEmpty())
                .forEach(configValue -> LOGGER.warn("ConfigValue '{}' has errors: {}", configValue.name(), configValue.errorMessages()));
        // Only if there are no config errors ...
        if (results.values().stream()
                .filter(
                        configValue -> !(configValue.name().equals(RelationalDatabaseConnectorConfig.TOPIC_PREFIX.name())
                                || configValue.name().equals(AbstractTopicNamingStrategy.TOPIC_HEARTBEAT_PREFIX.name())
                                || configValue.name().equals(SERVER_ID)))
                .allMatch(configValue -> configValue.errorMessages().isEmpty())) {
            // ... validate the connection too
            validateConnection(results, config);
        }

        return new Config(new ArrayList<>(results.values()));
    }

    /**
     * Validates connection to database.
     */
    protected abstract void validateConnection(Map<String, ConfigValue> configValues, Configuration config);

    /**
     * Connector-agnostic validation for the smart (multi-task) snapshot feature. When {@code smart.snapshot.enabled}
     * is on, the feature has hard prerequisites that are better caught at config time with a user-facing message than
     * surfaced later as silent single-task fallbacks or as tasks timing out at runtime:
     * <ul>
     *   <li>{@code tasks.max} must be greater than 1, otherwise there is only one task and nothing runs in parallel.</li>
     *   <li>A coordination bootstrap ({@code producer.override.bootstrap.servers}) must be configured, since the tasks
     *       coordinate the shared snapshot through a Kafka topic on that cluster.</li>
     * </ul>
     * The two internal snapshot timeouts are only warned about, as they are test-facing knobs whose defaults are
     * already correct. Connector-specific checks (snapshot mode, isolation level) are delegated to
     * {@link #validateSmartSnapshotMode(Configuration, Map)}.
     */
    protected void validateSmartSnapshotConfig(Configuration config, Map<String, ConfigValue> results) {
        if (!config.getBoolean(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED)) {
            return;
        }

        final int maxTasks = config.getInteger(TASKS_MAX_CONFIG, 1);
        if (maxTasks <= 1) {
            addSmartSnapshotError(results, TASKS_MAX_CONFIG,
                    "Smart snapshot (" + CommonConnectorConfig.SMART_SNAPSHOT_ENABLED.name() + "=true) requires '"
                            + TASKS_MAX_CONFIG + "' greater than 1 so tables can be snapshotted in parallel, but it is " + maxTasks
                            + ". Increase '" + TASKS_MAX_CONFIG + "' or disable smart snapshot.");
        }

        if (SnapshotCoordinationFacade.isCoordinationBootstrapMissing(config)) {
            addSmartSnapshotError(results, PRODUCER_BOOTSTRAP_OVERRIDE,
                    "Smart snapshot (" + CommonConnectorConfig.SMART_SNAPSHOT_ENABLED.name()
                            + "=true) requires a coordination bootstrap, but none is configured. Set '" + PRODUCER_BOOTSTRAP_OVERRIDE
                            + "' (the cluster hosting the snapshot coordination topic) or disable smart snapshot.");
        }

        // Warn-only: a task must not give up before the leader has waited for all tasks to join and prepared the
        // snapshot. Defaults are correct; these are internal, test-facing knobs, so a deliberately shortened timeout
        // must still be able to run.
        final long taskSnapshotInfoWaitMs = config.getLong(CommonConnectorConfig.SMART_SNAPSHOT_TASK_SNAPSHOT_INFO_WAIT_TIMEOUT_MS);
        final long leaderJoinWaitMs = config.getLong(CommonConnectorConfig.SMART_SNAPSHOT_LEADER_JOIN_WAIT_TIMEOUT_MS);
        if (taskSnapshotInfoWaitMs <= leaderJoinWaitMs) {
            LOGGER.warn("Misconfigured smart snapshot timeouts: the task snapshot-info wait ({}ms) is not larger than the "
                    + "leader join wait ({}ms), so tasks may give up before the leader publishes the snapshot info. Increase "
                    + "'{}' above '{}' plus the expected snapshot preparation time.",
                    taskSnapshotInfoWaitMs, leaderJoinWaitMs,
                    CommonConnectorConfig.SMART_SNAPSHOT_TASK_SNAPSHOT_INFO_WAIT_TIMEOUT_MS.name(),
                    CommonConnectorConfig.SMART_SNAPSHOT_LEADER_JOIN_WAIT_TIMEOUT_MS.name());
        }

        validateSmartSnapshotMode(config, results);
    }

    /**
     * Hook for connector-specific smart snapshot validation, such as which {@code snapshot.mode} values are supported
     * and the snapshot isolation level. These use connector-specific enums, so the base class cannot check them. Called
     * only when smart snapshot is enabled. The default implementation is a no-op.
     */
    protected void validateSmartSnapshotMode(Configuration config, Map<String, ConfigValue> results) {
    }

    private static void addSmartSnapshotError(Map<String, ConfigValue> results, String key, String message) {
        results.computeIfAbsent(key, ConfigValue::new).addErrorMessage(message);
    }
}
