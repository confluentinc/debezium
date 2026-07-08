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
import io.debezium.pipeline.source.snapshot.KafkaLogSnapshotCoordination;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager.SnapshotSetup;
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
    // Table-driven allocation (design §9): computed once in start() from the table count discovered by
    // SqlServerSnapshotLifecycleManager.prepareSnapshot(), independent of Connect's own tasks.max/maxTasks --
    // taskConfigs() reuses this exact value so the count it hands to the coordinator matches what was already
    // published in the shared snapshot-info record's NUM_TASKS field.
    private volatile int smartSnapshotEffectiveMaxTasks;

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

        // Best-effort early gate (Connect's authoritative maxTasks arrives later via taskConfigs(int)); avoids
        // doing anchor-capture work at all when the operator has smart snapshot enabled but tasks.max<=1.
        Integer maxTask = config.getInteger("tasks.max");
        if (maxTask != null && maxTask <= 1) {
            LOGGER.info("Smart snapshot is enabled but tasks.max <= 1, falling back to the ordinary snapshot path");
            return;
        }

        SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        if (!SnapshotCoordinationFacade.hasCoordinationBootstrap(config, connectorConfig)) {
            LOGGER.info("Smart snapshot: no coordination bootstrap configured, skipping smart snapshot setup");
            return;
        }

        // Phase 0 (design §0.5): smartSnapshotApplies() below already enforces database.names.size()==1.
        String database = connectorConfig.getDatabaseNames().get(0);
        if (alreadyStreaming(connectorConfig, database)) {
            LOGGER.info("Smart snapshot: [{}] already has a streaming offset, skipping smart snapshot", database);
            return;
        }

        SnapshotCoordinationFacade coordinationFacade = new SnapshotCoordinationFacade(config, connectorConfig);
        SmartSnapshotConnectorCoordinator coordinator = null;
        // Everything below starts real resources (the facade's Kafka clients, then the coordinator's daemon
        // monitor thread) but only "commits" them to smartSnapshotConnectorCoordinator at the very end. If
        // anything in between throws -- most realistically prepareSnapshot()'s awaitMaxLsn timing out on a
        // freshly-enabled CDC capture job, but also any coordination write -- those resources must be released
        // here: Connect calls stop() on a FAILED connector, but by then the field is still null so stop()
        // would be a no-op and the monitor thread + Kafka clients would leak (a fresh set on every restart).
        try {
            // Started here (rather than left for SmartSnapshotConnectorCoordinator#start to do internally) so
            // bumpEpochIfIncompleteRoundExists() below can peek at the coordination topic before the coordinator
            // ever reads-and-adopts an epoch. Idempotent: the coordinator's own later start() call becomes a
            // no-op against this already-started facade.
            coordinationFacade.start();
            bumpEpochIfIncompleteRoundExists(coordinationFacade, database);
            coordinator = new SmartSnapshotConnectorCoordinator(
                    coordinationFacade, context(), connectorConfig.getLogicalName(), connectorConfig.getSmartSnapshotMonitorPollIntervalMs());
            coordinator.start();
            if (coordinator.isComplete()) {
                LOGGER.info("Smart snapshot: [{}] round already complete, skipping", database);
                coordinator.stop();
                return;
            }

            // Capture L_db + discover captured tables synchronously, directly from Connector-side code -- unlike
            // Postgres's task-0 leader thread, SQL Server's anchor capture is a one-shot query with nothing held
            // afterward, so it has none of the long-lived-connection/DND concerns that motivated Postgres's move.
            boolean shouldStream = connectorConfig.getSnapshotMode() != SqlServerConnectorConfig.SnapshotMode.INITIAL_ONLY;
            SqlServerSnapshotLifecycleManager lifecycle = new SqlServerSnapshotLifecycleManager(connectorConfig, database, () -> connect(connectorConfig));
            SnapshotSetup setup = lifecycle.prepareSnapshot(shouldStream);
            if (setup.tables().isEmpty()) {
                LOGGER.info("Smart snapshot: [{}] no captured tables, skipping smart snapshot", database);
                coordinator.stop();
                return;
            }

            int effectiveMaxTasks = Math.max(1, ceilDiv(setup.tables().size(), connectorConfig.getSmartSnapshotTablesPerTask()));
            int epoch = coordinationFacade.readEpoch();
            coordinationFacade.writeSnapshotInfo(setup.snapshotName(), setup.consistentPosition(), epoch, setup.tables(), effectiveMaxTasks);
            LOGGER.info("Smart snapshot: [{}] published L_db={} epoch={} numTasks={} for {} table(s)",
                    database, setup.consistentPosition(), epoch, effectiveMaxTasks, setup.tables().size());

            publishUncapturedEligibleTables(config, connectorConfig, database, lifecycle.getUncapturedEligibleTables(), epoch);

            this.smartSnapshotConnectorCoordinator = coordinator;
            this.smartSnapshotEffectiveMaxTasks = effectiveMaxTasks;
        }
        catch (RuntimeException e) {
            // coordinator.stop() also stops the shared facade (and the monitor thread); if we failed before
            // the coordinator was even created, stop the facade directly.
            if (coordinator != null) {
                coordinator.stop();
            }
            else {
                coordinationFacade.stop();
            }
            throw e;
        }
    }

    /**
     * Piggyback record (design §6.2 leftover set, not part of the shared {@code SnapshotCoordinationFacade}'s
     * typed protocol): the schema-history writer (task.id==0) reads this to additionally dispatch tables that
     * are eligible for schema tracking but outside the data-capture set -- otherwise smart snapshot would
     * silently under-populate schema-history relative to single-task mode whenever
     * {@code store.only.captured.tables.ddl=false} (the default) and any table filtering is configured.
     *
     * <p>Published only synchronously from {@link #start} (and its epoch-bumping restart re-run), always
     * before any task exists -- so a task reading this record can rely on ordering: if it isn't present, there
     * is legitimately nothing to publish (design §19; the async {@code taskConfigs()} republish path that once
     * broke this ordering guarantee was scoped out for Phase 0). Skipped entirely when empty, which the reader
     * treats identically to "no leftover".
     */
    private void publishUncapturedEligibleTables(Configuration config, SqlServerConnectorConfig connectorConfig,
                                                 String database, List<TableId> uncapturedEligibleTables, int epoch) {
        if (uncapturedEligibleTables.isEmpty()) {
            return;
        }
        KafkaLogSnapshotCoordination coordination = new KafkaLogSnapshotCoordination(config, connectorConfig, false);
        try {
            coordination.start();
            coordination.write(SqlServerUncapturedSchemaCoordination.key(connectorConfig.getLogicalName()),
                    SqlServerUncapturedSchemaCoordination.value(uncapturedEligibleTables, epoch));
            LOGGER.info("Smart snapshot: [{}] published {} eligible-but-uncaptured table(s) for the schema-history writer, epoch={}",
                    database, uncapturedEligibleTables.size(), epoch);
        }
        catch (Exception e) {
            throw new DebeziumException("Smart snapshot: [" + database + "] failed to publish eligible-but-uncaptured tables", e);
        }
        finally {
            coordination.stop();
        }
    }

    static boolean smartSnapshotApplies(Configuration configuration) {
        SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(configuration);
        if (!connectorConfig.isSmartSnapshotEnabled()) {
            return false;
        }
        // Phase 0 (design §0.5): smart snapshot is restricted to single-database connectors. Multi-database
        // fan-out reintroduces the downscale-disruption problem (one DB's collapse reshuffles the connector-
        // wide task list by position, bouncing a still-active sibling DB's shards) with no mitigation built yet
        // -- deferred to Phase 1. Multi-DB connectors fall back to today's ordinary per-DB round-robin path.
        if (connectorConfig.getDatabaseNames().size() != 1) {
            return false;
        }
        switch (connectorConfig.getSnapshotMode()) {
            case INITIAL:
            case INITIAL_ONLY:
            case WHEN_NEEDED:
                return true; // parallelizable data snapshot
            case ALWAYS: // avoid the post-downscale double snapshot -> single-task
            case SCHEMA_ONLY: // deprecated alias of NO_DATA
            case NO_DATA: // no data copy -> nothing to parallelize
            case RECOVERY: // schema-only recovery path, not a data snapshot
            case CONFIGURATION_BASED: // not supported for smart snapshot yet
            case CUSTOM: // custom snapshotter semantics unknown -- don't assume parallelizable
            default:
                return false;
        }
    }

    /**
     * Gap fix: {@code start()} above unconditionally rediscovers tables and republishes {@code snapshot_info}
     * on every Connector restart while a round is incomplete -- whether that restart is a routine worker
     * rebalance/config touch, or the customer deliberately restarting the connector to pick up a table added
     * mid-snapshot (the design's own stated recovery path -- see design doc §7.7B). Republishing under the
     * SAME epoch number would be unsafe if the table set changed: {@code done}/{@code restart_needed} records
     * on the coordination topic are keyed by (taskId, epoch) only, so a task that already finished its shard
     * under the old table/shard layout could be misread as "done" for a differently-sharded new layout that
     * happens to reuse the same epoch. Bumping the epoch here, before
     * {@link SmartSnapshotConnectorCoordinator#start()} ever reads-and-adopts one, makes every such restart
     * unconditionally safe rather than relying on the table set happening not to have changed. Must run
     * strictly before {@code coordinator.start()}: that call reads the epoch once and caches it internally,
     * so a bump written afterward would be silently ignored by the coordinator's own task-config issuing.
     * SQL-Server-specific: unlike Postgres's task-0 leader (whose epoch is decided once by the Connector's
     * {@code taskConfigs()} and only ever bumped in response to an explicit {@code restart_needed} signal), a
     * plain Kafka Connect restart is invisible to Postgres's leader thread but not to
     * {@code SqlServerConnector.start()}, which Kafka Connect re-invokes on every connector restart.
     */
    private void bumpEpochIfIncompleteRoundExists(SnapshotCoordinationFacade coordinationFacade, String database) {
        Map<String, Object> snapshotInfo = coordinationFacade.readSnapshotInfo();
        if (snapshotInfo == null || Boolean.TRUE.equals(snapshotInfo.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY))) {
            // nothing published yet (first-ever start), or the round already finished -- nothing to bump
            return;
        }
        Integer existingEpoch = coordinationFacade.readEpoch();
        if (existingEpoch == null) {
            return;
        }
        int bumped = existingEpoch + 1;
        LOGGER.info("Smart snapshot: [{}] connector restarted with an incomplete round still published, bumping epoch {} -> {}",
                database, existingEpoch, bumped);
        coordinationFacade.writeEpoch(bumped);
    }

    /**
     * Backward-compat check (design §11 gap): {@link SmartSnapshotConnectorCoordinator#start} checks Connect's
     * offset store via a single-field {@code {"server": serverName}} key, but {@link SqlServerPartition}'s real
     * source-partition key is {@code {server, database}} -- that lookup never matches, so left unchecked it
     * would force a needless full re-snapshot the first time smart snapshot is enabled for an already-streaming
     * database. Do the correct two-field lookup ourselves before ever starting a coordinator.
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

    private static int ceilDiv(int numerator, int divisor) {
        return (numerator + divisor - 1) / divisor;
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
                List<Map<String, String>> configs = coordinator.taskConfigs(smartSnapshotEffectiveMaxTasks, properties);
                if (configs != null) {
                    return configs;
                }
            }
            // either the round was already complete (configs==null) or maxTasks==1 -- smart snapshot is off
            // for this round; release and fall through to the ordinary path
            coordinator.stop();
            smartSnapshotConnectorCoordinator = null;
        }

        try (SqlServerConnection connection = connect(config)) {
            return buildTaskConfigs(connection, config, maxTasks, config.getDatabaseNames());
        }
        catch (SQLException e) {
            throw new IllegalArgumentException("Could not build task configs", e);
        }
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
        if (smartSnapshotConnectorCoordinator != null) {
            smartSnapshotConnectorCoordinator.stop();
            smartSnapshotConnectorCoordinator = null;
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
