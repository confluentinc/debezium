/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.config.ConfigurationNames.TASK_ID_PROPERTY_NAME;

import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

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
import io.debezium.connector.postgresql.PostgresConnectorConfig.LogicalDecoder;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;
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

    public static final String COORDINATION_STATE_KEY = "snapshot.coordination.state";
    public static final String COORDINATION_STATE_NEW = "NEW";
    public static final String COORDINATION_STATE_RESTART = "RESTART";
    public static final String EPOCH_KEY = "epoch";
    public static final String RESTART_KEY = "restart_required";
    public static final String SNAPSHOT_NAME_KEY = "snapshot_name";

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConnector.class);
    public static final int READ_ONLY_SUPPORTED_VERSION = 13;
    private static final long MONITOR_POLL_INTERVAL_MS = 30_000;

    private Map<String, String> props;
    private Thread monitorThread;
    private volatile int lastNumTasks;

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

        Configuration config = Configuration.from(props);
        boolean smartSnapshotEnabled = config.getBoolean(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED);

        if (smartSnapshotEnabled) {
            startMonitorThread(config);
        }
    }

    /**
     * Returns task configurations. In smart snapshot mode ({@code smart.snapshot.enabled=true}
     * and {@code maxTasks > 1}):
     * <ul>
     *   <li>If snapshot already complete (checked via offset topic) → returns single config
     *     for streaming (downscale).</li>
     *   <li>Otherwise, distributes tables round-robin across tasks. Each task config includes
     *     {@code task.id}, {@code epoch}, and {@code snapshot.coordination.state}.</li>
     * </ul>
     *
     * <p>Epoch determination from the shared key {@code {"server":"<prefix>"}}:
     * <ul>
     *   <li>No shared key → first start → epoch=1, state=NEW</li>
     *   <li>Shared key with {@code restart_required=true} or {@code snapshot_completed=false}
     *     → previous round incomplete → epoch=previous+1, state=RESTART</li>
     *   <li>Shared key with {@code snapshot_completed=true} → caught by isSnapshotComplete
     *     above, returns single config</li>
     * </ul>
     *
     * <p>The epoch in the config serves three purposes: (1) tasks write it to per-task offsets
     * for monitor epoch matching, (2) makes configs differ across epochs so the Herder's
     * {@code taskConfigsChanged()} returns true and tasks are actually restarted, (3) followers
     * poll for coordination data with matching epoch.
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (props == null) {
            return Collections.emptyList();
        }

        Configuration config = Configuration.from(props);
        boolean smartSnapshotEnabled = config.getBoolean(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED);

        if (!smartSnapshotEnabled || maxTasks <= 1) {
            return Collections.singletonList(new HashMap<>(props));
        }

        if (isSnapshotComplete(config, maxTasks)) {
            LOGGER.info("Smart snapshot: Snapshot is complete, returning single task config for streaming");
            return Collections.singletonList(new HashMap<>(props));
        }

        // Validate heartbeat is enabled — required for coordination via offset topic
        Duration heartbeatInterval = config.getDuration(Heartbeat.HEARTBEAT_INTERVAL, ChronoUnit.MILLIS);
        if (heartbeatInterval.isZero()) {
            throw new DebeziumException("Smart snapshot: Heartbeat must be enabled (heartbeat.interval.ms > 0) when smart.snapshot=true and tasks.max > 1");
        }

        // Determine epoch and coordination state
        String serverName = config.getString(CommonConnectorConfig.TOPIC_PREFIX);
        Map<String, String> sharedPartition = Collect.hashMapOf("server", serverName);
        Map<String, Object> sharedOffset = context().offsetStorageReader().offset(sharedPartition);

        int epoch;
        String coordinationState;
        if (sharedOffset == null) {
            LOGGER.info("Smart snapshot: Processing taskConfigs, no shared key found first start");
            epoch = 1;
            coordinationState = COORDINATION_STATE_NEW;
        }
        else {
            Integer existingEpoch = readEpoch(sharedOffset);
            boolean snapshotCompleted = Boolean.TRUE.equals(sharedOffset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY));
            boolean restartRequired = Boolean.TRUE.equals(sharedOffset.get(PostgresConnector.RESTART_KEY));

            LOGGER.info("Smart snapshot: Processing taskConfigs, shared key exists. snapshot_completed={}, restart_required={}, epoch={}",
                    snapshotCompleted, restartRequired, existingEpoch);

            if (restartRequired || !snapshotCompleted) {
                epoch = (existingEpoch != null ? existingEpoch : 0) + 1;
                coordinationState = COORDINATION_STATE_RESTART;
            }
            else {
                epoch = existingEpoch != null ? existingEpoch : 1;
                coordinationState = COORDINATION_STATE_NEW;
            }
        }

        List<TableId> tables = getMatchingCollections(config);
        if (tables.isEmpty()) {
            LOGGER.warn("Smart snapshot: no matching tables found, falling back to single task");
            return Collections.singletonList(new HashMap<>(props));
        }

        tables.sort(Comparator.comparing(TableId::toString));

        int numTasks = Math.min(maxTasks, tables.size());
        this.lastNumTasks = numTasks;

        List<List<TableId>> tablesByTask = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            tablesByTask.add(new ArrayList<>());
        }
        for (int i = 0; i < tables.size(); i++) {
            tablesByTask.get(i % numTasks).add(tables.get(i));
        }

        List<Map<String, String>> taskConfigsList = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            String snapshotTables = tablesByTask.get(i).stream()
                    .map(TableId::toString)
                    .collect(Collectors.joining(","));

            Map<String, String> taskProps = new HashMap<>(props);
            taskProps.put(CommonConnectorConfig.SNAPSHOT_MODE_TABLES.name(), snapshotTables);
            taskProps.put(TASK_ID_PROPERTY_NAME, String.valueOf(i));
            taskProps.put(EPOCH_KEY, String.valueOf(epoch));
            taskProps.put(COORDINATION_STATE_KEY, coordinationState);

            LOGGER.info("Smart snapshot: task {} tables=[{}], epoch={}, state={}", i, snapshotTables, epoch, coordinationState);
            taskConfigsList.add(taskProps);
        }

        return taskConfigsList;
    }

    @Override
    public void stop() {
        this.props = null;
        stopMonitorThread();
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
                    catch (Exception e) {
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

    /**
     * Checks whether the multi-task snapshot is complete by reading the offset topic.
     * First checks the shared key {@code {"server":"<prefix>"}} for {@code snapshot_completed=true}
     * (fast path — covers single-task completion and post-downscale).
     * <br>Then checks all per-task keys {@code {"server":"<prefix>","task":"N"}} for completion with matching epoch.
     */
    private boolean isSnapshotComplete(Configuration config, int maxTasks) {
        String serverName = config.getString(CommonConnectorConfig.TOPIC_PREFIX);

        // Fast path: check shared key
        Map<String, String> sharedPartition = Collect.hashMapOf("server", serverName);
        Map<String, Object> sharedOffset = context().offsetStorageReader().offset(sharedPartition);
        if (sharedOffset != null &&
                Boolean.TRUE.equals(sharedOffset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY))) {
            LOGGER.info("Smart snapshot: shared key shows snapshot_completed=true");
            return true;
        }

        // Check all per-task keys with epoch matching
        Integer expectedEpoch = readEpoch(sharedOffset);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskPartition = Collect.hashMapOf(
                    "server", serverName,
                    "task",
                    String.valueOf(i));
            Map<String, Object> taskOffset = context().offsetStorageReader().offset(taskPartition);
            if (taskOffset == null ||
                    !Boolean.TRUE.equals(taskOffset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY))) {
                return false;
            }
            if (expectedEpoch != null) {
                Integer taskEpoch = readEpoch(taskOffset);
                if (!expectedEpoch.equals(taskEpoch)) {
                    return false;
                }
            }
        }

        LOGGER.info("Smart snapshot: all {} per-task offsets show snapshot_completed=true", maxTasks);
        return true;
    }

    /**
     * Starts a daemon thread that polls the offset topic every 30 seconds for restart signals
     * or snapshot completion. Actions taken:
     * <ul>
     *   <li>{@code restart_required=true} in the shared key → a task detected a stale snapshot.
     *     Calls {@code requestTaskReconfiguration()} and <b>continues polling</b> (does not exit)
     *     the monitor must survive across epochs since {@code start()} is not called again
     *     on reconfiguration.</li>
     *   <li>All per-task keys show {@code snapshot_completed=true} with matching epoch →
     *     all tasks finished. Calls {@code requestTaskReconfiguration()} and <b>exits</b>
     *     — downscale to streaming, no further monitoring needed.</li>
     * </ul>
     *
     * <p>After triggering reconfiguration, the Herder calls {@code taskConfigs()} which returns
     * either a new set of task configs (with incremented epoch on restart) or a single task
     * config (for downscale to streaming).
     */
    private void startMonitorThread(Configuration config) {
        String serverName = config.getString(CommonConnectorConfig.TOPIC_PREFIX);

        monitorThread = new Thread(() -> {
            LOGGER.info("Smart snapshot: Monitor thread started for {}", serverName);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(MONITOR_POLL_INTERVAL_MS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.info("Smart snapshot: Monitor thread interrupted");
                    return;
                }

                // It guards against the monitor thread running before taskConfigs() has been called
                if (lastNumTasks <= 0) {
                    continue;
                }

                // Check shared key for epoch change (stale task signaled restart)
                Map<String, String> sharedPartition = Collect.hashMapOf("server", serverName);
                Map<String, Object> sharedOffset = context().offsetStorageReader().offset(sharedPartition);

                if (sharedOffset != null) {
                    // Check for restart_required (stale task signaled restart)
                    if (Boolean.TRUE.equals(sharedOffset.get(PostgresConnector.RESTART_KEY))) {
                        LOGGER.info("Smart snapshot: Monitor thread detected restart_required in shared key, requesting reconfiguration");
                        context().requestTaskReconfiguration();
                        continue;
                    }
                }

                // Check all per-task keys for completion with epoch matching
                boolean allComplete = true;
                Integer expectedEpoch = readEpoch(sharedOffset);
                for (int i = 0; i < lastNumTasks; i++) {
                    Map<String, String> taskPartition = Collect.hashMapOf("server", serverName, "task", String.valueOf(i));
                    Map<String, Object> taskOffset = context().offsetStorageReader().offset(taskPartition);
                    if (taskOffset == null) {
                        LOGGER.debug("Smart snapshot: Monitor thread detected task-{} has no offset yet", i);
                        allComplete = false;
                        break;
                    }

                    boolean taskCompleted = Boolean.TRUE.equals(taskOffset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY));
                    Integer taskEpoch = readEpoch(taskOffset);
                    LOGGER.info("Smart snapshot: Monitor thread detected task-{} snapshot_completed={}, epoch={} (expected={})", i, taskCompleted, taskEpoch,
                            expectedEpoch);

                    if (!taskCompleted) {
                        allComplete = false;
                        break;
                    }

                    if (expectedEpoch != null && !expectedEpoch.equals(taskEpoch)) {
                        LOGGER.warn("Smart snapshot: Monitor detected task-{} epoch mismatch (task={}, expected={})", i, taskEpoch, expectedEpoch);
                        allComplete = false;
                        break;
                    }
                }

                if (allComplete) {
                    LOGGER.info("Smart snapshot: Monitor thread detected all {} tasks completed snapshot, requesting task reconfiguration", lastNumTasks);
                    context().requestTaskReconfiguration();
                    return;
                }
            }
        }, "postgres-smart-snapshot-monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    private void stopMonitorThread() {
        if (monitorThread != null) {
            monitorThread.interrupt();
            try {
                monitorThread.join(5000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            monitorThread = null;
        }
    }

    static Integer readEpoch(@Nullable Map<String, Object> sharedOffset) {
        return (sharedOffset != null && sharedOffset.get(EPOCH_KEY) != null) ? ((Number) sharedOffset.get(EPOCH_KEY)).intValue() : null;
    }
}
