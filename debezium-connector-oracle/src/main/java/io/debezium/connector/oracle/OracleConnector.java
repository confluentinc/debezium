/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.common.SnapshotTableDistributor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

public class OracleConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnector.class);

    private Map<String, String> properties;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return OracleConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (properties == null) {
            return Collections.emptyList();
        }

        // For single task or when maxTasks is 1, use current behavior
        if (maxTasks <= 1) {
            return Collections.singletonList(new HashMap<>(properties));
        }

        // Resolve all tables matching include/exclude lists
        List<TableId> allTables = getMatchingCollections(Configuration.from(properties));

        // If no tables found or only one table, fall back to single task
        if (allTables.isEmpty() || allTables.size() == 1) {
            return Collections.singletonList(new HashMap<>(properties));
        }

        // Determine actual number of tasks (can't have more tasks than tables)
        int numTasks = Math.min(maxTasks, allTables.size());

        // Distribute tables via round-robin
        List<List<TableId>> tablesByTask = SnapshotTableDistributor.distributeRoundRobin(allTables, numTasks);

        // Generate task configs
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            Map<String, String> taskProps = new HashMap<>(properties);
            taskProps.put(CommonConnectorConfig.TASK_ID, String.valueOf(i));

            // Build assigned tables list for this task (explicit names, no regex)
            String assignedTables = tablesByTask.get(i).stream()
                    .map(TableId::identifier)
                    .collect(Collectors.joining(","));

            if (i == 0) {
                // Task 0: Snapshots its assigned subset, but streams ALL tables
                // - Keep original table.include.list for streaming (filters events for all tables)
                // - Use snapshot.include.collection.list to limit snapshot to assigned tables only
                taskProps.put(CommonConnectorConfig.SNAPSHOT_MODE_TABLES.name(), assignedTables);
                // Remove exclude list to ensure streaming captures all tables matching include list
                taskProps.remove(RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST.name());
                LOGGER.info("Task 0 will snapshot {} tables: [{}], then stream all tables matching original filter",
                        tablesByTask.get(i).size(), assignedTables);
            }
            else {
                // Tasks 1-N: Snapshot their assigned tables only, no streaming
                // Override table.include.list with assigned tables
                taskProps.put(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name(), assignedTables);
                // Remove exclude list since include is now explicit
                taskProps.remove(RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST.name());
                // Snapshot-only mode to avoid log mining conflicts
                taskProps.put(OracleConnectorConfig.SNAPSHOT_MODE.name(),
                        OracleConnectorConfig.SnapshotMode.INITIAL_ONLY.getValue());
            }

            taskConfigs.add(taskProps);
        }

        LOGGER.info("Configured {} snapshot tasks for {} tables (task-0 will stream all tables, tasks 1-{} are snapshot-only)",
                numTasks, allTables.size(), numTasks - 1);
        return taskConfigs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return OracleConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final ConfigValue databaseValue = configValues.get(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        if (!databaseValue.errorMessages().isEmpty()) {
            return;
        }

        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        final ConfigValue userValue = configValues.get(RelationalDatabaseConnectorConfig.USER.name());

        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        try (OracleConnection connection = new OracleConnection(connectorConfig.getJdbcConfig())) {
            LOGGER.debug("Successfully tested connection for {} with user '{}'", OracleConnection.connectionString(connectorConfig.getJdbcConfig()),
                    connection.username());
        }
        catch (SQLException | RuntimeException e) {
            LOGGER.error("Failed testing connection for {} with user '{}'", config.withMaskedPasswords(), userValue, e);
            hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(OracleConnectorConfig.ALL_FIELDS);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TableId> getMatchingCollections(Configuration config) {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final String databaseName = connectorConfig.getCatalogName();

        try (OracleConnection connection = new OracleConnection(connectorConfig.getJdbcConfig(), false)) {
            if (!Strings.isNullOrBlank(connectorConfig.getPdbName())) {
                connection.setSessionToPdb(connectorConfig.getPdbName());
            }
            // @TODO: we need to expose a better method from the connector, particularly getAllTableIds
            // the following's performance is acceptable when using PDBs but not as ideal with non-PDB
            return connection.readTableNames(databaseName, null, null, new String[]{ "TABLE" }).stream()
                    .filter(tableId -> connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                    .collect(Collectors.toList());
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }
}
