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
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
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
    private volatile SmartSnapshotConnectorCoordinator smartSnapshotConnectorCoordinator;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));

        Configuration config = Configuration.from(properties);
        if (!smartSnapshotApplies(config)) {
            return;
        }
        Integer maxTask = config.getInteger("tasks.max");
        if (maxTask == null || maxTask <= 1) {
            LOGGER.info("Smart snapshot is enabled but tasks.max <= 1, falling back to the ordinary snapshot path");
            return;
        }
        SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        if (!SnapshotCoordinationFacade.hasCoordinationBootstrap(config)) {
            LOGGER.info("Smart snapshot: no coordination bootstrap configured, skipping smart snapshot setup");
            return;
        }
        String database = connectorConfig.getDatabaseNames().get(0); // single-DB enforced by smartSnapshotApplies()
        if (alreadyStreaming(connectorConfig, database)) {
            LOGGER.info("Smart snapshot: [{}] already has a streaming offset, skipping smart snapshot", database);
            return;
        }

        // Discovery, L_db capture, and snapshot_info publish happen in the task-0 leader thread
        // (SqlServerConnectorTask), not here -- the leader runs in a DND-protected task, so a rebalance-induced
        // connector restart never bounces an in-flight round. This only stands up the coordinator (the
        // epoch/monitor state machine); the epoch bumps solely on a task-signaled restart_needed.
        SnapshotCoordinationFacade coordinationFacade = new SnapshotCoordinationFacade(config, connectorConfig);
        SmartSnapshotConnectorCoordinator coordinator = new SmartSnapshotConnectorCoordinator(
                coordinationFacade, context(), connectorConfig.getLogicalName(),
                connectorConfig.getSmartSnapshotMonitorPollIntervalMs(),
                connectorConfig.getSmartSnapshotReconfigurationTimeoutMs(), SqlServerConnector.class.getName());
        coordinator.start();
        if (coordinator.isComplete()) {
            LOGGER.info("Smart snapshot: [{}] round already complete, skipping", database);
            coordinator.stop();
            return;
        }
        this.smartSnapshotConnectorCoordinator = coordinator;
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

        SmartSnapshotConnectorCoordinator coordinator = this.smartSnapshotConnectorCoordinator;
        if (coordinator != null) {
            if (maxTasks > 1) {
                List<Map<String, String>> configs = coordinator.taskConfigs(maxTasks, properties);
                if (configs != null) {
                    return configs;
                }
            }
            // round already complete (configs==null) or maxTasks==1 -- release and fall through to ordinary path
            coordinator.stop();
            smartSnapshotConnectorCoordinator = null;
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
        if (smartSnapshotConnectorCoordinator != null) {
            smartSnapshotConnectorCoordinator.stop();
            smartSnapshotConnectorCoordinator = null;
        }
    }

    static boolean smartSnapshotApplies(Configuration configuration) {
        SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(configuration);
        if (!connectorConfig.isSmartSnapshotEnabled()) {
            return false;
        }
        // Phase 0: single database only. Multi-DB fan-out reshuffles the connector-wide task list by position
        // when one DB downscales, bouncing a sibling DB's still-active shards -- unsolved, deferred.
        if (connectorConfig.getDatabaseNames().size() != 1) {
            return false;
        }
        // Phase 0: repeatable_read (the default) and read_committed. Cross-task consistency comes from the
        // single L_db anchor + CDC catch-up + last-writer-wins PK upsert, not from the snapshot isolation
        // level, so read_committed -- which still never reads uncommitted data -- is safe here.
        // read_uncommitted is unsafe (dirty rows that can be rolled back can't be reconciled from L_db);
        // snapshot/exclusive are not validated for the single-anchor model. See isSmartSnapshotSupportedIsolationMode.
        if (!isSmartSnapshotSupportedIsolationMode(connectorConfig.getSnapshotIsolationMode())) {
            return false;
        }
        switch (connectorConfig.getSnapshotMode()) {
            case INITIAL:
            case INITIAL_ONLY:
            case WHEN_NEEDED:
                return true; // parallelizable data snapshot
            case ALWAYS:
            case SCHEMA_ONLY:
            case NO_DATA:
            case RECOVERY:
            case CONFIGURATION_BASED:
            case CUSTOM:
            default:
                return false;
        }
    }

    /**
     * Snapshot isolation modes for which smart snapshot (multi-task) is supported in Phase 0: {@code
     * repeatable_read} (the connector default) and {@code read_committed}. Both never read uncommitted data,
     * so the single-{@code L_db}-anchor + CDC-catch-up + last-writer-wins PK upsert model reconciles any rows
     * that change during the parallel scan. {@code read_uncommitted} is excluded (dirty rows that can be
     * rolled back can't be reconciled from {@code L_db}); {@code snapshot}/{@code exclusive} are not validated.
     */
    static boolean isSmartSnapshotSupportedIsolationMode(SqlServerConnectorConfig.SnapshotIsolationMode mode) {
        return mode == SqlServerConnectorConfig.SnapshotIsolationMode.REPEATABLE_READ
                || mode == SqlServerConnectorConfig.SnapshotIsolationMode.READ_COMMITTED;
    }

    /**
     * Fail config validation when smart-snapshot multi-task is configured (feature enabled + a coordination
     * bootstrap set) with an isolation mode Phase 0 does not support (anything other than repeatable_read or
     * read_committed) -- surfaced at connector-create time rather than silently falling back to single-task.
     */
    private void validateSmartSnapshotIsolationMode(Map<String, ConfigValue> configValues, Configuration config,
                                                    SqlServerConnectorConfig connectorConfig) {
        if (!connectorConfig.isSmartSnapshotEnabled()
                || !SnapshotCoordinationFacade.hasCoordinationBootstrap(config)
                || isSmartSnapshotSupportedIsolationMode(connectorConfig.getSnapshotIsolationMode())) {
            return;
        }
        ConfigValue isolationValue = configValues.get(SqlServerConnectorConfig.SNAPSHOT_ISOLATION_MODE.name());
        if (isolationValue != null) {
            isolationValue.addErrorMessage("Smart snapshot (multi-task) currently supports only the '"
                    + SqlServerConnectorConfig.SnapshotIsolationMode.REPEATABLE_READ.getValue() + "' and '"
                    + SqlServerConnectorConfig.SnapshotIsolationMode.READ_COMMITTED.getValue()
                    + "' snapshot isolation modes; got '" + connectorConfig.getSnapshotIsolationMode().getValue() + "'.");
        }
    }

    /**
     * True if this database already has a completed/streaming offset. The shared coordinator only checks a
     * single-field {@code {server}} offset key, which never matches {@link SqlServerPartition}'s real
     * {@code {server, database}} key, so we do the correct two-field lookup here to avoid a needless
     * re-snapshot when smart snapshot is first enabled on an already-streaming database.
     */
    private boolean alreadyStreaming(SqlServerConnectorConfig connectorConfig, String database) {
        SourceConnectorContext sourceContext = (SourceConnectorContext) context();
        Map<String, String> partitionKey = new SqlServerPartition(connectorConfig.getLogicalName(), database).getSourcePartition();
        Map<String, Object> existingOffset = sourceContext.offsetStorageReader().offset(partitionKey);
        if (existingOffset == null) {
            return false;
        }
        Object snapshot = existingOffset.get(AbstractSourceInfo.SNAPSHOT_KEY);
        boolean completed = Boolean.TRUE.equals(existingOffset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY));
        boolean initialSnapshotRunning = snapshot != null && !completed;
        return !initialSnapshotRunning;
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
        validateSmartSnapshotIsolationMode(configValues, config, sqlServerConfig);
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
