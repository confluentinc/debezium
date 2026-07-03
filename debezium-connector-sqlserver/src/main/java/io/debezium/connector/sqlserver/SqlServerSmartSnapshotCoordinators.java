/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.config.ConfigurationNames.TASK_ID_PROPERTY_NAME;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.KafkaLogSnapshotCoordination;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.source.snapshot.SnapshotLifecycleManager.SnapshotSetup;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

/**
 * Owns the SQL Server side of the Connector-managed smart-snapshot flow: one {@link SmartSnapshotConnectorCoordinator}
 * + dedicated coordination topic per database (design §3.5, §11.1), table-driven allocation (design §9), and the
 * {@code task.id} (global identity) vs {@code smart.snapshot.database.task.index} (per-DB coordination index)
 * split (design §4.5).
 *
 * <p>Unlike the current Postgres implementation, the snapshot anchor ({@code L_db}) is captured here, directly
 * from Connector-side code, rather than from a task0 leader thread -- see the design doc §3.4/§4.2 footnote for
 * why that's safe for SQL Server's one-shot, nothing-held {@code fn_cdc_get_max_lsn()} capture.
 *
 * <p>After {@link #start}, {@link #perDatabase} contains only databases that still need a sharded round: a
 * database that turns out to already be fully streaming (backward-compat check) or whose smart-snapshot round
 * was already completed is dropped immediately, mirroring how {@code PostgresConnector.start()} nulls out its
 * (single) coordinator once complete.
 */
public class SqlServerSmartSnapshotCoordinators {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSmartSnapshotCoordinators.class);

    private static class PerDatabaseState {
        final SmartSnapshotConnectorCoordinator coordinator;
        final List<TableId> tables;

        PerDatabaseState(SmartSnapshotConnectorCoordinator coordinator, List<TableId> tables) {
            this.coordinator = coordinator;
            this.tables = tables;
        }
    }

    // preserves database iteration order for deterministic task.id assignment
    private final Map<String, PerDatabaseState> perDatabase = new LinkedHashMap<>();

    public static boolean smartSnapshotApplies(SqlServerConnectorConfig connectorConfig, Configuration rawConfig) {
        if (!connectorConfig.isSmartSnapshotEnabled()) {
            return false;
        }
        // smart.snapshot.enabled defaults to true connector-wide (it also gates lighter-weight single-task
        // optimizations unrelated to multi-task fan-out -- DND delay, larger batches). The coordination
        // topic's bootstrap servers are documented as "required when smart.snapshot.enabled=true and
        // tasks.max > 1" -- treat their absence as "operator hasn't opted into multi-task fan-out" rather
        // than attempting Kafka client construction with a null bootstrap and failing hard (e.g. embedded
        // engine tests, which have no coordination topic Kafka broker at all).
        if (!coordinationBootstrapServersConfigured(connectorConfig, rawConfig)) {
            return false;
        }
        switch (connectorConfig.getSnapshotMode()) {
            case INITIAL:
            case INITIAL_ONLY:
            case WHEN_NEEDED:
                return true;                        // parallelizable data snapshot
            case ALWAYS:                            // avoid the post-downscale double snapshot -> single-task
            case SCHEMA_ONLY:                       // deprecated alias of NO_DATA
            case NO_DATA:                           // no data copy -> nothing to parallelize
            case RECOVERY:                          // schema-only recovery path, not a data snapshot
            case CONFIGURATION_BASED:                // not supported for smart snapshot yet
            case CUSTOM:                             // custom snapshotter semantics unknown -- don't assume parallelizable
            default:
                return false;
        }
    }

    // package-private: also used by SqlServerConnectorTask's streaming-offset-seeding gate
    static boolean coordinationBootstrapServersConfigured(SqlServerConnectorConfig connectorConfig, Configuration rawConfig) {
        String explicit = connectorConfig.getSmartSnapshotCoordinationBootstrapServers();
        if (explicit != null && !explicit.isEmpty()) {
            return true;
        }
        String producerOverride = rawConfig.subset("producer.override.", true).getString(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        return producerOverride != null && !producerOverride.isEmpty();
    }

    /**
     * Databases this connector is configured for, minus whichever ended up needing a sharded round
     * (i.e. minus {@link #perDatabase}'s keys after {@link #start}). Callers should fall back to the
     * ordinary single-task-per-database path for these.
     */
    public List<String> remainingDatabases(SqlServerConnectorConfig connectorConfig) {
        return connectorConfig.getDatabaseNames().stream()
                .filter(db -> !perDatabase.containsKey(db))
                .collect(Collectors.toList());
    }

    public boolean hasActiveDatabases() {
        return !perDatabase.isEmpty();
    }

    /**
     * Per database: skip it if it's already genuinely streaming (backward-compat, §11 gap); otherwise start its
     * coordinator, and if a sharded round is still needed, capture L_db synchronously and publish it -- exactly
     * reproducing approach2's capture-then-publish-then-tasks-read ordering, using the current framework's
     * public surface (no fork of {@link SmartSnapshotConnectorCoordinator} needed).
     */
    public void start(SqlServerConnectorConfig connectorConfig, Configuration rawConfig, ConnectorContext connectorContext,
                      Supplier<SqlServerConnection> connectionSupplier) {
        start(connectorConfig, connectorContext, connectionSupplier,
                database -> {
                    String topicName = connectorConfig.getLogicalName() + "." + database + ".snapshot-coordination";
                    String clientIdSuffix = connectorConfig.getLogicalName() + "-" + database + "-coordination-connector";
                    Map<String, Object> clientConfig = KafkaLogSnapshotCoordination.clientConfigFromOverrides(
                            rawConfig, connectorConfig.getSmartSnapshotCoordinationBootstrapServers());
                    return new KafkaLogSnapshotCoordination(clientConfig, topicName, clientIdSuffix);
                });
    }

    // package-private: lets tests substitute an in-memory SnapshotCoordination instead of a real Kafka topic
    void start(SqlServerConnectorConfig connectorConfig, ConnectorContext connectorContext,
              Supplier<SqlServerConnection> connectionSupplier, Function<String, SnapshotCoordination> coordinationFactory) {
        for (String database : connectorConfig.getDatabaseNames()) {
            try {
                if (alreadyStreaming(connectorContext, connectorConfig, database)) {
                    LOGGER.info("Smart snapshot: [{}] already has a streaming offset, skipping smart snapshot for this database", database);
                    continue;
                }

                List<TableId> tables = matchingCollections(connectorConfig, connectionSupplier, database);
                if (tables.isEmpty()) {
                    LOGGER.info("Smart snapshot: [{}] no captured tables, skipping smart snapshot for this database", database);
                    continue;
                }

                SnapshotCoordination coordination = coordinationFactory.apply(database);

                SmartSnapshotConnectorCoordinator coordinator = new SmartSnapshotConnectorCoordinator(
                        coordination, connectorContext, connectorConfig.getLogicalName());
                coordinator.start(tables);

                if (coordinator.isComplete()) {
                    LOGGER.info("Smart snapshot: [{}] round already complete, skipping", database);
                    coordinator.stop();
                    continue;
                }

                captureAndPublishAnchor(connectorConfig, database, tables, coordinator, coordination, connectionSupplier);
                perDatabase.put(database, new PerDatabaseState(coordinator, tables));
            }
            catch (RuntimeException e) {
                stop();
                throw new DebeziumException("Smart snapshot: [" + database + "] failed to start coordination", e);
            }
        }
    }

    private void captureAndPublishAnchor(SqlServerConnectorConfig connectorConfig, String database, List<TableId> tables,
                                         SmartSnapshotConnectorCoordinator coordinator, SnapshotCoordination coordination,
                                         Supplier<SqlServerConnection> connectionSupplier) {
        boolean shouldStream = connectorConfig.getSnapshotMode() != SqlServerConnectorConfig.SnapshotMode.INITIAL_ONLY;
        SqlServerSnapshotLifecycleManager lifecycle = new SqlServerSnapshotLifecycleManager(database, connectionSupplier);
        SnapshotSetup setup = lifecycle.prepareSnapshot(tables, shouldStream);

        Map<String, Object> shared = new HashMap<>();
        shared.put(SmartSnapshotConnectorCoordinator.SLOT_LSN_KEY, setup.consistentPosition());
        shared.put(SmartSnapshotConnectorCoordinator.SNAPSHOT_NAME_KEY, setup.snapshotName());
        shared.put(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY, false);
        shared.put(SmartSnapshotConnectorCoordinator.EPOCH_KEY, coordinator.currentEpoch());
        try {
            coordination.write(Collect.hashMapOf("server", connectorConfig.getLogicalName()), shared);
        }
        catch (Exception e) {
            throw new DebeziumException("Smart snapshot: [" + database + "] failed to publish L_db to coordination topic", e);
        }
        LOGGER.info("Smart snapshot: [{}] published L_db={} epoch={} for {} table(s)",
                database, setup.consistentPosition(), coordinator.currentEpoch(), tables.size());
    }

    /**
     * Backward-compat check (design §11 gap): {@link SmartSnapshotConnectorCoordinator#start} checks Connect's
     * offset store via a single-field {@code {"server": serverName}} key, but {@link SqlServerPartition}'s real
     * source-partition key is {@code {server, database}} -- that lookup never matches, so left unchecked it
     * would force a needless full re-snapshot the first time smart snapshot is enabled for an already-streaming
     * database. Do the correct two-field lookup ourselves before ever starting a coordinator for this database.
     */
    private boolean alreadyStreaming(ConnectorContext connectorContext, SqlServerConnectorConfig connectorConfig, String database) {
        SourceConnectorContext sourceContext = (SourceConnectorContext) connectorContext;
        Map<String, String> partitionKey = new SqlServerPartition(connectorConfig.getLogicalName(), database).getSourcePartition();
        Map<String, Object> existingOffset = sourceContext.offsetStorageReader().offset(partitionKey);
        if (existingOffset == null) {
            return false;
        }
        Object snapshot = existingOffset.get(io.debezium.connector.AbstractSourceInfo.SNAPSHOT_KEY);
        boolean completed = Boolean.TRUE.equals(existingOffset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY));
        boolean initialSnapshotRunning = snapshot != null && !completed;
        return !initialSnapshotRunning;
    }

    private static List<TableId> matchingCollections(SqlServerConnectorConfig connectorConfig,
                                                      Supplier<SqlServerConnection> connectionSupplier, String database) {
        try (SqlServerConnection connection = connectionSupplier.get()) {
            return connection.readTableNames(database, null, null, new String[]{ "TABLE" }).stream()
                    .filter(tableId -> connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                    .collect(Collectors.toList());
        }
        catch (SQLException e) {
            throw new DebeziumException("Smart snapshot: [" + database + "] failed to list captured tables", e);
        }
    }

    /**
     * Table-driven allocation (design §9): {@code effectiveMaxTasks = ceil(tableCount / tablesPerTask)}, passed
     * as the {@code maxTasks} argument to the (unmodified) shared coordinator -- since
     * {@code effectiveMaxTasks <= tableCount} always, {@code min(effectiveMaxTasks, tableCount)} is a no-op
     * passthrough, so table-driven allocation needs no core change.
     *
     * <p>Relabels each config's per-DB-local {@code task.id} (stamped by the shared coordinator) into
     * {@code smart.snapshot.database.task.index}; the caller is responsible for the final, connector-wide
     * {@code task.id} assignment across this method's output plus whatever it merges in for
     * {@link #remainingDatabases}.
     */
    public List<Map<String, String>> taskConfigs(int tablesPerTask, Map<String, String> baseProps) {
        List<Map<String, String>> out = new ArrayList<>();
        Iterator<Map.Entry<String, PerDatabaseState>> iterator = perDatabase.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, PerDatabaseState> entry = iterator.next();
            String database = entry.getKey();
            PerDatabaseState state = entry.getValue();
            int effectiveMaxTasks = Math.max(1, ceilDiv(state.tables.size(), tablesPerTask));

            Map<String, String> dbBaseProps = new HashMap<>(baseProps);
            dbBaseProps.put(SqlServerConnectorConfig.DATABASE_NAMES.name(), database);

            List<Map<String, String>> dbConfigs = state.coordinator.taskConfigs(effectiveMaxTasks, dbBaseProps);
            if (dbConfigs == null) {
                // this round just completed (coordinator.taskConfigs() itself wrote the completion record) --
                // release it and drop out of perDatabase so remainingDatabases() picks this DB up from now on
                LOGGER.info("Smart snapshot: [{}] round complete, releasing coordinator", database);
                state.coordinator.stop();
                iterator.remove();
                continue;
            }
            for (Map<String, String> config : dbConfigs) {
                String localIndex = config.get(TASK_ID_PROPERTY_NAME);
                config.put(SqlServerConnectorConfig.SMART_SNAPSHOT_DATABASE_TASK_INDEX_PROPERTY_NAME, localIndex);
                out.add(config);
            }
        }
        return out;
    }

    private static int ceilDiv(int numerator, int divisor) {
        return (numerator + divisor - 1) / divisor;
    }

    public void stop() {
        for (PerDatabaseState state : perDatabase.values()) {
            state.coordinator.stop();
        }
        perDatabase.clear();
    }
}
