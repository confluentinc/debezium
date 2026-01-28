/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

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
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.postgresql.core.ServerVersion;
import org.postgresql.core.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.common.SnapshotTableDistributor;
import io.debezium.connector.postgresql.PostgresConnectorConfig.LogicalDecoder;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.util.ThreadNameContext;
import io.debezium.util.Threads;

/**
 * A Kafka Connect source connector that creates tasks which use Postgresql streaming replication off a logical replication slot
 * to receive incoming changes for a database and publish them to Kafka.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link PostgresConnectorConfig}.
 *
 * @author Horia Chiorean
 */
public class PostgresConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConnector.class);
    public static final int READ_ONLY_SUPPORTED_VERSION = 13;

    private Map<String, String> props;

    public PostgresConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PostgresConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (props == null) {
            return Collections.emptyList();
        }

        // For single task or when maxTasks is 1, use current behavior
        if (maxTasks <= 1) {
            return Collections.singletonList(new HashMap<>(props));
        }

        // Resolve all tables matching include/exclude lists
        List<TableId> allTables = getMatchingCollections(Configuration.from(props));

        // If no tables found or only one table, fall back to single task
        if (allTables.isEmpty() || allTables.size() == 1) {
            return Collections.singletonList(new HashMap<>(props));
        }

        // Determine actual number of tasks (can't have more tasks than tables)
        int numTasks = Math.min(maxTasks, allTables.size());

        // Distribute tables via round-robin
        List<List<TableId>> tablesByTask = SnapshotTableDistributor.distributeRoundRobin(allTables, numTasks);

        // Generate task configs
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            Map<String, String> taskProps = new HashMap<>(props);
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
                // Snapshot-only mode to avoid replication slot conflicts
                taskProps.put(PostgresConnectorConfig.SNAPSHOT_MODE.name(),
                        PostgresConnectorConfig.SnapshotMode.INITIAL_ONLY.getValue());
            }

            taskConfigs.add(taskProps);
        }

        LOGGER.info("Configured {} snapshot tasks for {} tables (task-0 will stream all tables, tasks 1-{} are snapshot-only)",
                numTasks, allTables.size(), numTasks - 1);
        return taskConfigs;
    }

    @Override
    public void stop() {
        this.props = null;
    }

    @Override
    public ConfigDef config() {
        return PostgresConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final ConfigValue databaseValue = configValues.get(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        final ConfigValue slotNameValue = configValues.get(PostgresConnectorConfig.SLOT_NAME.name());
        final ConfigValue pluginNameValue = configValues.get(PostgresConnectorConfig.PLUGIN_NAME.name());
        if (!databaseValue.errorMessages().isEmpty() || !slotNameValue.errorMessages().isEmpty()
                || !pluginNameValue.errorMessages().isEmpty()) {
            return;
        }

        final PostgresConnectorConfig postgresConfig = new PostgresConnectorConfig(config);
        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        final ConfigValue portValue = configValues.get(PostgresConnectorConfig.PORT.name());
        final ConfigValue userValue = configValues.get(PostgresConnectorConfig.USER.name());
        final ConfigValue passwordValue = configValues.get(PostgresConnectorConfig.PASSWORD.name());
        Duration timeout = postgresConfig.getConnectionValidationTimeout();
        ThreadNameContext threadNameContext = ThreadNameContext.from(postgresConfig);
        // Try to connect to the database ...
        try {
            Threads.runWithTimeout(PostgresConnector.class, () -> {
                try (PostgresConnection connection = new PostgresConnection(postgresConfig.getJdbcConfig(),
                        PostgresConnection.CONNECTION_VALIDATE_CONNECTION, threadNameContext)) {
                    try {
                        // Prepare connection without initial statement execution
                        connection.connection(false);
                        testConnection(connection);
                        checkReadOnlyMode(connection, postgresConfig);
                        checkLoginReplicationRoles(connection);
                        if (LogicalDecoder.PGOUTPUT.equals(postgresConfig.plugin())) {
                            int pgversion = checkPostgresVersionForPgoutputSupport(connection, postgresConfig);
                            if (ServerVersion.v10.getVersionNum() > pgversion) {
                                final String errorMessage = "PGOUTPUT plugin is only supported on postgres server version 10+";
                                LOGGER.error(errorMessage);
                                hostnameValue.addErrorMessage(errorMessage);
                                pluginNameValue.addErrorMessage(errorMessage);
                            }
                        }
                    }
                    catch (SQLException e) {
                        LOGGER.error("Failed testing connection for {} with user '{}'", connection.connectionString(),
                                connection.username(), e);
                        hostnameValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
                        databaseValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
                        portValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
                        userValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
                        passwordValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
                    }
                }
            }, timeout, postgresConfig.getLogicalName(), "connection-validation", threadNameContext);
        }
        catch (TimeoutException e) {
            hostnameValue.addErrorMessage("Connection validation timed out after " + timeout.toMillis() + " ms");
        }
        catch (Exception e) {
            hostnameValue.addErrorMessage("Error during connection validation: " + e.getMessage());
        }
    }

    private static void checkReadOnlyMode(PostgresConnection connection, PostgresConnectorConfig postgresConfig) throws SQLException {

        ServerInfo serverInfo = connection.serverInfo();

        if (postgresConfig.isReadOnlyConnection() && serverInfo.version() < READ_ONLY_SUPPORTED_VERSION) {
            throw new DebeziumException(String.format("Read only is not supported for version minor to %s", READ_ONLY_SUPPORTED_VERSION));
        }
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig) {
        return ExactlyOnceSupport.SUPPORTED;
    }

    private static void checkLoginReplicationRoles(PostgresConnection connection) throws SQLException {
        if (!connection.queryAndMap(
                "SELECT r.rolcanlogin AS rolcanlogin, r.rolreplication AS rolreplication," +
                // for AWS the user might not have directly the rolreplication rights, but can be assigned
                // to one of those role groups: rds_superuser, rdsadmin or rdsrepladmin
                        " CAST(array_position(ARRAY(SELECT b.rolname" +
                        " FROM pg_catalog.pg_auth_members m" +
                        " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                        " WHERE m.member = r.oid), 'rds_superuser') AS BOOL) IS TRUE AS aws_superuser" +
                        ", CAST(array_position(ARRAY(SELECT b.rolname" +
                        " FROM pg_catalog.pg_auth_members m" +
                        " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                        " WHERE m.member = r.oid), 'rdsadmin') AS BOOL) IS TRUE AS aws_admin" +
                        ", CAST(array_position(ARRAY(SELECT b.rolname" +
                        " FROM pg_catalog.pg_auth_members m" +
                        " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                        " WHERE m.member = r.oid), 'rdsrepladmin') AS BOOL) IS TRUE AS aws_repladmin" +
                        ", CAST(array_position(ARRAY(SELECT b.rolname" +
                        " FROM pg_catalog.pg_auth_members m" +
                        " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                        " WHERE m.member = r.oid), 'rds_replication') AS BOOL) IS TRUE AS aws_replication" +
                        " FROM pg_roles r WHERE r.rolname = current_user",
                connection.singleResultMapper(rs -> rs.getBoolean("rolcanlogin")
                        && (rs.getBoolean("rolreplication")
                                || rs.getBoolean("aws_superuser")
                                || rs.getBoolean("aws_admin")
                                || rs.getBoolean("aws_repladmin")
                                || rs.getBoolean("aws_replication")),
                        "Could not fetch roles"))) {
            final String errorMessage = "Postgres roles LOGIN and REPLICATION are not assigned to user: " + connection.username();
            LOGGER.error(errorMessage);
        }
    }

    private static void testConnection(PostgresConnection connection) throws SQLException {
        connection.execute("SELECT version()");
        LOGGER.info("Successfully tested connection for {} with user '{}'", connection.connectionString(),
                connection.username());
    }

    private static int checkPostgresVersionForPgoutputSupport(PostgresConnection connection, PostgresConnectorConfig postgresConfig) throws SQLException {
        // check for DB version and LogicalDecoder compatibility
        final Version dbVersion = ServerVersion.from(
                connection.queryAndMap(
                        "SHOW server_version",
                        connection.singleResultMapper(
                                rs -> rs.getString("server_version"),
                                "Could not fetch db version")));
        return dbVersion.getVersionNum();
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(PostgresConnectorConfig.ALL_FIELDS);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TableId> getMatchingCollections(Configuration config) {
        PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(config);
        ThreadNameContext threadNameContext = ThreadNameContext.from(connectorConfig);
        try (PostgresConnection connection = new PostgresConnection(connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL,
                threadNameContext)) {
            return connection.readTableNames(connectorConfig.databaseName(), null, null, new String[]{ "TABLE" }).stream()
                    .filter(tableId -> connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                    .collect(Collectors.toList());
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }
}
