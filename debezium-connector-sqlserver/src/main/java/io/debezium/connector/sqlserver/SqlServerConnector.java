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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
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
import io.debezium.pipeline.source.snapshot.KafkaLogSnapshotCoordination;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.source.snapshot.SnapshotLifecycleManager;
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

    /*
     * Smart-snapshot (multi-task) coordinators, one per database. SQL Server is multi-database, so each
     * database coordinates its own snapshot independently (own consistent LSN, own write barrier, own epoch).
     * Empty/null when smart.snapshot is disabled or every database's snapshot is already complete.
     */
    private volatile Map<String, SmartSnapshotConnectorCoordinator> smartSnapshotCoordinators;

    /*
     * Captured-table count per database, recorded at start(). Used in taskConfigs() to derive the snapshot
     * task count (ceil(count / smart.snapshot.tables.per.task)) independently of the user's tasks.max.
     */
    private volatile Map<String, Integer> smartSnapshotDbTableCounts;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));

        final Configuration config = Configuration.from(properties);
        if (!config.getBoolean(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED)) {
            return;
        }

        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);

        // Smart snapshot relies on SQL Server SNAPSHOT isolation: sharded tasks open their own
        // snapshot-isolation transaction (and skip table locking) to pin the Connector-captured L_db. Under
        // any other mode a task would read current committed state and silently ignore L_db, producing an
        // inconsistent snapshot. Fail fast rather than corrupt data.
        if (connectorConfig.getSnapshotIsolationMode() != SqlServerConnectorConfig.SnapshotIsolationMode.SNAPSHOT) {
            throw new DebeziumException("smart.snapshot.enabled=true requires snapshot.isolation.mode=snapshot (and the "
                    + "database option ALLOW_SNAPSHOT_ISOLATION ON), but snapshot.isolation.mode="
                    + connectorConfig.getSnapshotIsolationMode().getValue());
        }
        // Fan-out returns ceil(#tables / tables.per.task) configs per database, which routinely exceeds
        // tasks.max; AK >= 3.7 fails the connector for that unless tasks.max.enforce=false.
        if (!"false".equalsIgnoreCase(properties.getOrDefault("tasks.max.enforce", "true"))) {
            LOGGER.warn("smart.snapshot fan-out can return more tasks than tasks.max; set tasks.max.enforce=false "
                    + "or the Kafka Connect runtime (>= 3.7) will reject the connector.");
        }

        final String serverName = connectorConfig.getLogicalName();
        final String bootstrapServers = connectorConfig.getSmartSnapshotCoordinationBootstrapServers();
        final boolean shouldStream = connectorConfig.getSnapshotMode() != SqlServerConnectorConfig.SnapshotMode.INITIAL_ONLY;

        // All captured tables across all databases (catalog-qualified), grouped per database below.
        final List<TableId> allTables = getMatchingCollections(config);
        final Map<String, SmartSnapshotConnectorCoordinator> coordinators = new ConcurrentHashMap<>();
        final Map<String, Integer> tableCounts = new ConcurrentHashMap<>();

        for (String databaseName : connectorConfig.getDatabaseNames()) {
            List<TableId> dbTables = allTables.stream()
                    .filter(t -> databaseName.equals(t.catalog()))
                    .collect(Collectors.toList());
            if (dbTables.isEmpty()) {
                continue;
            }
            tableCounts.put(databaseName, dbTables.size());

            String coordinationTopic = serverName + "." + databaseName + ".snapshot-coordination";
            String clientIdSuffix = serverName + "-" + databaseName + "-coordination-connector";
            SnapshotCoordination coordination = new KafkaLogSnapshotCoordination(bootstrapServers, coordinationTopic, clientIdSuffix);
            SnapshotLifecycleManager lifecycle = new SqlServerSnapshotLifecycleManager(connectorConfig, databaseName);
            SmartSnapshotConnectorCoordinator coordinator = new SmartSnapshotConnectorCoordinator(
                    coordination, lifecycle, context(), serverName, databaseName);

            coordinator.start(dbTables, shouldStream);
            if (coordinator.isComplete()) {
                coordinator.stop();
            }
            else {
                coordinators.put(databaseName, coordinator);
            }
        }

        this.smartSnapshotCoordinators = coordinators;
        this.smartSnapshotDbTableCounts = tableCounts;
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

        // Smart snapshot (multi-task) path: while any database's snapshot is still active, return the
        // per-database sharded task configs from each coordinator. Each task is DB-exclusive (database.names
        // is pinned to its single database). Once every coordinator reports complete (all return null), we
        // fall through to the normal per-database streaming layout below (downscale).
        if (Configuration.from(properties).getBoolean(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED) && maxTasks > 1
                && smartSnapshotCoordinators != null && !smartSnapshotCoordinators.isEmpty()) {
            final boolean shouldStream = config.getSnapshotMode() != SqlServerConnectorConfig.SnapshotMode.INITIAL_ONLY;
            final int tablesPerTask = config.getSmartSnapshotTablesPerTask();
            final List<Map<String, String>> smartConfigs = new ArrayList<>();
            for (Map.Entry<String, SmartSnapshotConnectorCoordinator> entry : smartSnapshotCoordinators.entrySet()) {
                Map<String, String> baseProps = new HashMap<>(properties);
                // DB-exclusive: every task this coordinator emits serves only its single database.
                baseProps.put(DATABASE_NAMES.name(), entry.getKey());
                // Table-driven count: ceil(#tables / tablesPerTask), independent of the user's tasks.max
                // (the snapshot is temporarily wider than tasks.max — requires tasks.max.enforce=false, and
                // streaming downscales back to the user's tasks.max). Passed as the coordinator's maxTasks so
                // its min(maxTasks, #tables) yields exactly the derived count.
                int tableCount = smartSnapshotDbTableCounts.getOrDefault(entry.getKey(), 0);
                int effectiveMaxTasks = Math.max(1, (int) Math.ceil((double) tableCount / tablesPerTask));
                List<Map<String, String>> dbConfigs = entry.getValue().taskConfigs(effectiveMaxTasks, baseProps, shouldStream);
                if (dbConfigs != null) {
                    smartConfigs.addAll(dbConfigs);
                }
            }
            if (!smartConfigs.isEmpty()) {
                return smartConfigs;
            }
            // all coordinators complete → downscale to the normal streaming layout
        }

        try (SqlServerConnection connection = connect(config)) {
            return buildTaskConfigs(connection, config, maxTasks);
        }
        catch (SQLException e) {
            throw new IllegalArgumentException("Could not build task configs", e);
        }
    }

    private List<Map<String, String>> buildTaskConfigs(SqlServerConnection connection, SqlServerConnectorConfig config,
                                                       int maxTasks) {
        List<String> databaseNames = config.getDatabaseNames();

        // Initialize the database list for each task
        List<List<String>> databasesByTask = new ArrayList<>();
        final int numTasks = Math.min(maxTasks, config.getDatabaseNames().size());
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
        Map<String, SmartSnapshotConnectorCoordinator> coordinators = this.smartSnapshotCoordinators;
        this.smartSnapshotCoordinators = null;
        if (coordinators != null) {
            coordinators.values().forEach(SmartSnapshotConnectorCoordinator::stop);
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

    private SqlServerConnection connect(SqlServerConnectorConfig sqlServerConfig) {
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
