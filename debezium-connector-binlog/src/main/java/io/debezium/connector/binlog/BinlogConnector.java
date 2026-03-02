/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.common.SnapshotTableDistributor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.util.ThreadNameContext;
import io.debezium.util.Threads;

/**
 * Abstract base class for binlog-based connectors.
 *
 * @author Chris Cranford
 */
public abstract class BinlogConnector<T extends BinlogConnectorConfig> extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnector.class);

    @Immutable
    private Map<String, String> properties;

    @Override
    public void start(Map<String, String> props) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));
    }

    @Override
    public void stop() {
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
                // Snapshot-only mode to avoid binlog position conflicts
                taskProps.put(BinlogConnectorConfig.SNAPSHOT_MODE.name(),
                        BinlogConnectorConfig.SnapshotMode.INITIAL_ONLY.getValue());
            }

            taskConfigs.add(taskProps);
        }

        LOGGER.info("Configured {} snapshot tasks for {} tables (task-0 will stream all tables, tasks 1-{} are snapshot-only)",
                numTasks, allTables.size(), numTasks - 1);
        return taskConfigs;
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        final T connectorConfig = createConnectorConfig(config);
        Duration timeout = connectorConfig.getConnectionValidationTimeout();
        ThreadNameContext threadNameContext = ThreadNameContext.from(connectorConfig);

        try {
            Threads.runWithTimeout(this.getClass(), () -> {
                try (BinlogConnectorConnection connection = createConnection(config, connectorConfig, threadNameContext)) {
                    try {
                        connection.connect();
                        connection.execute("SELECT version()");
                        LOGGER.info("Successfully tested connection for {} with user '{}'",
                                connection.connectionString(), connection.connectionConfig().username());
                    }
                    catch (SQLException e) {
                        LOGGER.error("Failed testing connection for {} with user '{}'",
                                connection.connectionString(), connection.connectionConfig().username(), e);
                        hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage()
                                + (e.getCause() != null && e.getCause().getMessage() != null ? " Caused by: " + e.getCause().getMessage() : ""));
                    }
                }
                catch (SQLException e) {
                    LOGGER.error("Unexpected error shutting down the database connection", e);
                }
            }, timeout, connectorConfig.getLogicalName(), "connection-validation", threadNameContext);
        }
        catch (TimeoutException e) {
            hostnameValue.addErrorMessage("Connection validation timed out after " + timeout.toMillis() + " ms");
        }
        catch (Exception e) {
            hostnameValue.addErrorMessage("Error during connection validation: " + e.getMessage());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<TableId> getMatchingCollections(Configuration config) {
        final T connectorConfig = createConnectorConfig(config);
        try (BinlogConnectorConnection connection = createConnection(config, connectorConfig, ThreadNameContext.from(connectorConfig))) {
            final List<TableId> tables = new ArrayList<>();
            final List<String> databaseNames = connection.availableDatabases();
            final RelationalTableFilters tableFilter = connectorConfig.getTableFilters();
            for (String databaseName : databaseNames) {
                if (!tableFilter.databaseFilter().test(databaseName)) {
                    continue;
                }
                tables.addAll(
                        connection.readTableNames(databaseName, null, null, new String[]{ "TABLE" }).stream()
                                .filter(tableId -> tableFilter.dataCollectionFilter().isIncluded(tableId))
                                .collect(Collectors.toList()));
            }
            return tables;
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

    /**
     * Create the connection.
     *
     * @param config the connector configuration; never null
     * @param connectorConfig the connector configuration; never null
     * @return the connector connection; never null
     */
    protected abstract BinlogConnectorConnection createConnection(Configuration config, T connectorConfig, ThreadNameContext threadNameContext);

    /**
     * Create the connector configuration.
     *
     * @param config the configuration; never null
     * @return the connector-specific configuration
     */
    protected abstract T createConnectorConfig(Configuration config);
}
