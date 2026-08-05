/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnector;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotLockingMode;
import io.debezium.connector.mysql.jdbc.MySqlConnection;
import io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration;
import io.debezium.connector.mysql.jdbc.MySqlFieldReaderResolver;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.util.ThreadNameContext;

/**
 * A Kafka Connect source connector that creates tasks that read the MySQL binary log and generate the corresponding
 * data change events.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link MySqlConnectorConfig}.
 *
 *
 * @author Randall Hauch
 */
public class MySqlConnector extends BinlogConnector<MySqlConnectorConfig> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnector.class);

    // The set of snapshot modes with a parallelizable initial data snapshot.
    private static final Set<String> PARALLELIZABLE_SNAPSHOT_MODES = Set.of(
            SnapshotMode.INITIAL.getValue(),
            SnapshotMode.INITIAL_ONLY.getValue(),
            SnapshotMode.WHEN_NEEDED.getValue());

    private Map<String, String> props;
    private volatile SmartSnapshotConnectorCoordinator smartSnapshotConnectorCoordinator;

    public MySqlConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MySqlConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        this.props = props;

        Configuration config = Configuration.from(props);
        if (smartSnapshotApplies(config)) {
            Integer maxTask = config.getInteger("tasks.max");
            if (maxTask != null && maxTask <= 1) {
                LOGGER.info("Smart snapshot: [role=connector] Enabled but tasks.max is 1 or less, falling back to feature-disabled behaviour");
                return;
            }
            MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);

            if (!SnapshotCoordinationFacade.hasCoordinationBootstrap(config)) {
                LOGGER.info("Smart snapshot: [role=connector] No coordination bootstrap configured; skipping smart snapshot setup in start()");
                return;
            }

            SnapshotCoordinationFacade coordinationFacade = new SnapshotCoordinationFacade(config, connectorConfig);
            smartSnapshotConnectorCoordinator = new SmartSnapshotConnectorCoordinator(coordinationFacade, context(),
                    connectorConfig.getLogicalName(), connectorConfig.getSmartSnapshotMonitorPollIntervalMs(),
                    connectorConfig.getContextName());

            smartSnapshotConnectorCoordinator.start();

            SmartSnapshotConnectorCoordinator oldCoordinator = this.smartSnapshotConnectorCoordinator;
            if (oldCoordinator.isComplete()) {
                smartSnapshotConnectorCoordinator = null;
                oldCoordinator.stop();
            }
        }
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (props == null) {
            return Collections.emptyList();
        }

        Configuration config = Configuration.from(props);
        SmartSnapshotConnectorCoordinator coordinator = this.smartSnapshotConnectorCoordinator;
        if (smartSnapshotApplies(config) && coordinator != null && maxTasks > 1) {
            List<Map<String, String>> taskConfigs = coordinator.taskConfigs(maxTasks, props);
            if (!coordinator.isComplete()) {
                return taskConfigs;
            }
            // Snapshot complete: the coordinator returned the single streaming config. Drop and stop the coordinator.
            smartSnapshotConnectorCoordinator = null;
            coordinator.stop();
            return taskConfigs;
        }

        // Feature not applicable, or maxTasks == 1: stop the coordinator if present and hand out a single config.
        if (coordinator != null) {
            smartSnapshotConnectorCoordinator = null;
            coordinator.stop();
        }
        return Collections.singletonList(new HashMap<>(props));
    }

    @Override
    public void stop() {
        super.stop();
        this.props = null;
        if (smartSnapshotConnectorCoordinator != null) {
            smartSnapshotConnectorCoordinator.stop();
        }
    }

    @Override
    public ConfigDef config() {
        return MySqlConnectorConfig.configDef();
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(MySqlConnectorConfig.ALL_FIELDS);
    }

    @Override
    protected MySqlConnection createConnection(Configuration config, MySqlConnectorConfig connectorConfig, ThreadNameContext threadNameContext) {
        return new MySqlConnection(
                new MySqlConnectionConfiguration(config),
                MySqlFieldReaderResolver.resolve(connectorConfig), ThreadNameContext.from(connectorConfig));
    }

    @Override
    protected MySqlConnectorConfig createConnectorConfig(Configuration config) {
        return new MySqlConnectorConfig(config);
    }

    // visible for testing
    static boolean smartSnapshotApplies(Configuration configuration) {
        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(configuration);
        if (!connectorConfig.isSmartSnapshotEnabled()) {
            return false;
        }
        // Only the initial data-snapshot modes are parallelizable.
        if (!PARALLELIZABLE_SNAPSHOT_MODES.contains(connectorConfig.getSnapshotMode().getValue())) {
            return false;
        }
        // v1: only 'minimal' locking gives a short, correct cross-connection shared point. Everything else
        // (extended / none / percona) degrades to the single-task path.
        return connectorConfig.getSnapshotLockingMode()
                .map(mode -> mode == SnapshotLockingMode.MINIMAL)
                .orElse(false);
    }
}
