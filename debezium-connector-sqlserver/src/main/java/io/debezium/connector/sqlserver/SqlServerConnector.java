/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.config.ConfigurationNames.TASK_ID_PROPERTY_NAME;
import static io.debezium.connector.sqlserver.SqlServerConnectorConfig.DATABASE_NAMES;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.util.ThreadNameContext;
import io.debezium.util.Threads;

/**
 * The main connector class used to instantiate configuration and execution classes
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnector.class);

    private Map<String, String> properties;
    private volatile SqlServerSmartSnapshotCoordinators smartSnapshotCoordinators;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));

        final SqlServerConnectorConfig config = new SqlServerConnectorConfig(Configuration.from(properties));
        if (SqlServerSmartSnapshotCoordinators.smartSnapshotApplies(config, Configuration.from(properties))) {
            SqlServerSmartSnapshotCoordinators coordinators = new SqlServerSmartSnapshotCoordinators();
            coordinators.start(config, Configuration.from(properties), context(), () -> connect(config));
            this.smartSnapshotCoordinators = coordinators.hasActiveDatabases() ? coordinators : null;
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SqlServerConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1 && !properties.containsKey(DATABASE_NAMES.name())) {
            throw new IllegalArgumentException("Only a single connector task may be started in single-partition mode");
        }

        final SqlServerConnectorConfig config = new SqlServerConnectorConfig(Configuration.from(properties));

        if (smartSnapshotCoordinators != null && maxTasks > 1) {
            List<Map<String, String>> shardedConfigs = smartSnapshotCoordinators.taskConfigs(
                    config.getSmartSnapshotTablesPerTask(), properties);
            List<String> remainingDatabases = smartSnapshotCoordinators.remainingDatabases(config);

            List<Map<String, String>> merged = new ArrayList<>(shardedConfigs);
            if (!remainingDatabases.isEmpty()) {
                try (SqlServerConnection connection = connect(config)) {
                    merged.addAll(buildTaskConfigs(connection, config, maxTasks, remainingDatabases));
                }
                catch (SQLException e) {
                    throw new IllegalArgumentException("Could not build task configs", e);
                }
            }
            for (int i = 0; i < merged.size(); i++) {
                merged.set(i, withGlobalTaskId(merged.get(i), i));
            }

            if (!smartSnapshotCoordinators.hasActiveDatabases()) {
                smartSnapshotCoordinators.stop();
                smartSnapshotCoordinators = null;
            }
            return merged;
        }

        if (smartSnapshotCoordinators != null) {
            // maxTasks == 1 -- smart snapshot is off for this round; release and fall through
            smartSnapshotCoordinators.stop();
            smartSnapshotCoordinators = null;
        }

        try (SqlServerConnection connection = connect(config)) {
            return buildTaskConfigs(connection, config, maxTasks, config.getDatabaseNames());
        }
        catch (SQLException e) {
            throw new IllegalArgumentException("Could not build task configs", e);
        }
    }

    private static Map<String, String> withGlobalTaskId(Map<String, String> config, int globalTaskId) {
        Map<String, String> withId = new HashMap<>(config);
        withId.put(TASK_ID_PROPERTY_NAME, String.valueOf(globalTaskId));
        return Collections.unmodifiableMap(withId);
    }

    private List<Map<String, String>> buildTaskConfigs(SqlServerConnection connection, SqlServerConnectorConfig config,
                                                       int maxTasks, List<String> databaseNames) {
        if (databaseNames.isEmpty()) {
            return Collections.emptyList();
        }

        // Initialize the database list for each task
        List<List<String>> databasesByTask = new ArrayList<>();
        final int numTasks = Math.min(maxTasks, databaseNames.size());
        for (int i = 0; i < numTasks; i++) {
            databasesByTask.add(new ArrayList<>());
        }

        // Add each database to a task list via round-robin.
        for (int databaseNameIndex = 0; databaseNameIndex < databaseNames.size(); databaseNameIndex++) {
            int taskIndex = databaseNameIndex % numTasks;
            String realDatabaseName = connection.retrieveRealDatabaseName(databaseNames.get(databaseNameIndex));
            databasesByTask.get(taskIndex).add(realDatabaseName);
        }

        // Create a task config for each task, assigning each a list of database names.
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int taskIndex = 0; taskIndex < numTasks; taskIndex++) {
            String taskDatabases = String.join(",", databasesByTask.get(taskIndex));
            Map<String, String> taskProperties = new HashMap<>(properties);
            taskProperties.put(SqlServerConnectorConfig.DATABASE_NAMES.name(), taskDatabases);
            taskProperties.put(TASK_ID_PROPERTY_NAME, String.valueOf(taskIndex));
            taskConfigs.add(Collections.unmodifiableMap(taskProperties));
        }

        return taskConfigs;
    }

    @Override
    public void stop() {
        if (smartSnapshotCoordinators != null) {
            smartSnapshotCoordinators.stop();
            smartSnapshotCoordinators = null;
        }
    }

    @Override
    public ConfigDef config() {
        return SqlServerConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        if (!configValues.get(DATABASE_NAMES.name()).errorMessages().isEmpty()) {
            return;
        }

        final SqlServerConnectorConfig sqlServerConfig = new SqlServerConnectorConfig(config);
        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        final ConfigValue userValue = configValues.get(RelationalDatabaseConnectorConfig.USER.name());
        final boolean isCredentialProviderConfigured = sqlServerConfig.isCredentialProviderConfigured();
        ThreadNameContext threadNameContext = ThreadNameContext.from(sqlServerConfig);
        Duration timeout = sqlServerConfig.getConnectionValidationTimeout();
        // Try to connect to the database ...
        try {
            Threads.runWithTimeout(SqlServerConnector.class, () -> {
                try (SqlServerConnection connection = connect(sqlServerConfig)) {
                    connection.execute("SELECT @@VERSION");
                    String username = connection.username();
                    if (isCredentialProviderConfigured) {
                        LOGGER.debug("Successfully tested connection for {} using access token authentication", connection.connectionString());
                    }
                    else {
                        LOGGER.debug("Successfully tested connection for {} with user '{}'", connection.connectionString(), username);
                    }
                    LOGGER.info("Checking database existence and connected principal's access to CDC table based on "
                            + "configured snapshot mode");
                    final List<String> noAccessDatabaseNames = new ArrayList<>();
                    for (String databaseName : sqlServerConfig.getDatabaseNames()) {
                        if (sqlServerConfig.getSnapshotMode() == SqlServerConnectorConfig.SnapshotMode.INITIAL_ONLY) {
                            connection.retrieveRealDatabaseName(databaseName);
                        }
                        else {
                            if (!connection.checkIfConnectedUserHasAccessToCDCTable(databaseName)) {
                                noAccessDatabaseNames.add(databaseName);
                            }
                        }
                    }
                    if (!noAccessDatabaseNames.isEmpty()) {
                        String principalDescription = isCredentialProviderConfigured
                                ? "token-identified principal"
                                : "User " + config.getString(RelationalDatabaseConnectorConfig.USER);
                        String entityType = isCredentialProviderConfigured ? "principal" : "user";
                        String errorMessage = String.format(
                                "%s does not have access to CDC schema in the following databases: %s. This %s can only be used in initial_only snapshot mode",
                                principalDescription, String.join(", ", noAccessDatabaseNames), entityType);
                        LOGGER.error(errorMessage);
                        userValue.addErrorMessage(errorMessage);
                    }
                }
                catch (Exception e) {
                    if (isCredentialProviderConfigured) {
                        LOGGER.error("Failed testing connection using access token authentication", e);
                    }
                    else {
                        LOGGER.error("Failed testing connection for {} with user '{}'", config.withMaskedPasswords(),
                                userValue, e);
                    }
                    hostnameValue.addErrorMessage("Unable to connect. Check this and other connection properties. Error: "
                            + e.getMessage());
                }
            }, timeout, sqlServerConfig.getLogicalName(), "connection-validation", threadNameContext);
        }
        catch (TimeoutException e) {
            hostnameValue.addErrorMessage("Connection validation timed out after " + timeout.toMillis() + " ms");
        }
        catch (Exception e) {
            hostnameValue.addErrorMessage("Error during connection validation: " + e.getMessage());
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(SqlServerConnectorConfig.ALL_FIELDS);
    }

    static SqlServerConnection connect(SqlServerConnectorConfig sqlServerConfig) {
        return new SqlServerConnection(sqlServerConfig, null, Collections.emptySet(),
                sqlServerConfig.useSingleDatabase());
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TableId> getMatchingCollections(Configuration config) {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        final List<String> databaseNames = connectorConfig.getDatabaseNames();

        try (SqlServerConnection connection = connect(connectorConfig)) {
            List<TableId> tables = new ArrayList<>();
            databaseNames.forEach(databaseName -> {
                try {
                    tables.addAll(
                            connection.readTableNames(databaseName, null, null, new String[]{ "TABLE" }).stream()
                                    .filter(tableId -> connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                                    .collect(Collectors.toList()));
                }
                catch (SQLException e) {
                    throw new DebeziumException(e);
                }
            });

            return tables;
        }
        catch (SQLException e) {
            throw new RuntimeException("Could not retrieve real database name", e);
        }
    }
}
