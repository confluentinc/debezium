/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.sqlserver.metrics.SqlServerMetricsFactory;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.GuardrailValidator;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager.SnapshotSetup;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination.MissingTopicPolicy;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * The main task executing streaming from SQL Server.
 * Responsible for lifecycle management the streaming code.
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerConnectorTask extends BaseSourceTask<SqlServerPartition, SqlServerOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnectorTask.class);
    private static final String CONTEXT_NAME = "sql-server-connector-task";

    private volatile SqlServerTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile SqlServerConnection dataConnection;
    private volatile SqlServerConnection metadataConnection;
    private volatile SqlServerErrorHandler errorHandler;
    private volatile SqlServerDatabaseSchema schema;
    private volatile SnapshotCoordinationFacade smartSnapshotCoordination;
    private volatile Thread smartSnapshotLeaderThread;
    // task-0's leader thread stashes the eligible-but-uncaptured leftover set here before publishing
    // snapshot_info; task-0's shard coordinator reads it (in-memory, same task) after seeing snapshot_info.
    private volatile List<TableId> smartSnapshotUncapturedTables = List.of();

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<SqlServerPartition, SqlServerOffsetContext> start(Configuration config) {
        final Clock clock = Clock.system();

        // By default do not load whole result sets into memory
        config = config.edit()
                .withDefault(CommonConnectorConfig.DRIVER_CONFIG_PREFIX + "responseBuffering", "adaptive")
                .withDefault(CommonConnectorConfig.DRIVER_CONFIG_PREFIX + "fetchSize", 10_000)
                .build();

        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        final SqlServerValueConverters valueConverters = new SqlServerValueConverters(connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(), connectorConfig.binaryHandlingMode());

        MainConnectionProvidingConnectionFactory<SqlServerConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new SqlServerConnection(connectorConfig,
                        valueConverters, connectorConfig.getSkippedOperations(), connectorConfig.useSingleDatabase(), connectorConfig.getOptionRecompile()));
        dataConnection = connectionFactory.mainConnection();
        metadataConnection = new SqlServerConnection(connectorConfig, valueConverters,
                connectorConfig.getSkippedOperations(), connectorConfig.useSingleDatabase());

        this.schema = new SqlServerDatabaseSchema(connectorConfig, metadataConnection.getDefaultValueConverter(), valueConverters, topicNamingStrategy,
                schemaNameAdjuster);
        this.schema.initializeStorage();
        taskContext = new SqlServerTaskContext(connectorConfig, schema);

        Offsets<SqlServerPartition, SqlServerOffsetContext> offsets = getPreviousOffsets(
                new SqlServerPartition.Provider(connectorConfig),
                new SqlServerOffsetContext.Loader(connectorConfig));

        seedStreamingOffsetsFromCoordination(connectorConfig, offsets);

        // Manual Bean Registration
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, metadataConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverters);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, offsets);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);

        // Service providers
        registerServiceProviders(connectorConfig.getServiceRegistry());

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        // Validate guardrail limits for captured tables to prevent loading excessive table schemas into memory
        if (connectorConfig.getGuardrailCollectionsMax() <= 0) {
            LOGGER.info("Guardrail validation skipped");
        }
        else {
            validateGuardrailLimits(connectorConfig, dataConnection);
        }

        validateSchemaHistory(connectorConfig, dataConnection::validateLogPosition, offsets, schema,
                snapshotterService.getSnapshotter());

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new SqlServerErrorHandler(connectorConfig, queue, errorHandler);

        final SqlServerEventMetadataProvider metadataProvider = new SqlServerEventMetadataProvider();

        SignalProcessor<SqlServerPartition, SqlServerOffsetContext> signalProcessor = new SignalProcessor<>(
                SqlServerConnector.class, connectorConfig, Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                offsets);

        final EventDispatcher<SqlServerPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                connectorConfig.createHeartbeat(
                        topicNamingStrategy,
                        schemaNameAdjuster,
                        connectionFactory::newConnection,
                        exception -> {
                            final String sqlErrorId = exception.getMessage();
                            throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                        }),
                schemaNameAdjuster,
                signalProcessor,
                connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

        NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        Integer smartSnapshotEpoch = connectorConfig.getSmartSnapshotEpoch();
        ChangeEventSourceCoordinator<SqlServerPartition, SqlServerOffsetContext> coordinator;
        if (connectorConfig.isSmartSnapshotEnabled() && smartSnapshotEpoch != null) {
            // Sharded smart-snapshot task. Phase 0 is single-DB, so task.id doubles as the shard index.
            String taskId = connectorConfig.getTaskId();
            this.smartSnapshotCoordination = new SnapshotCoordinationFacade(config, connectorConfig);
            // task-0 is the leader: discover + capture L_db + publish snapshot_info on a background thread
            // (a DND-protected task, so a rebalance-induced connector restart never bounces the round).
            if ("0".equals(taskId)) {
                startSmartSnapshotLeader(config, connectorConfig, smartSnapshotEpoch);
            }
            coordinator = new SqlServerSmartSnapshotChangeEventSourceCoordinator(
                    offsets,
                    errorHandler,
                    SqlServerConnector.class,
                    connectorConfig,
                    new SqlServerChangeEventSourceFactory(connectorConfig, connectionFactory, metadataConnection, errorHandler, dispatcher, clock, schema,
                            notificationService, snapshotterService),
                    new SqlServerMetricsFactory(offsets.getPartitions()),
                    dispatcher,
                    schema,
                    clock,
                    signalProcessor,
                    notificationService,
                    snapshotterService,
                    smartSnapshotEpoch,
                    smartSnapshotCoordination,
                    taskId,
                    () -> smartSnapshotUncapturedTables, // task-0's uncaptured leftover, stashed by its leader
                    () -> new SqlServerConnection(connectorConfig, valueConverters, connectorConfig.getSkippedOperations(),
                            connectorConfig.useSingleDatabase(), connectorConfig.getOptionRecompile()));
        }
        else {
            coordinator = new SqlServerChangeEventSourceCoordinator(
                    offsets,
                    errorHandler,
                    SqlServerConnector.class,
                    connectorConfig,
                    new SqlServerChangeEventSourceFactory(connectorConfig, connectionFactory, metadataConnection, errorHandler, dispatcher, clock, schema,
                            notificationService, snapshotterService),
                    new SqlServerMetricsFactory(offsets.getPartitions()),
                    dispatcher,
                    schema,
                    clock,
                    signalProcessor,
                    notificationService,
                    snapshotterService);
        }

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    /**
     * Spawns the task-0 leader background thread: discover tables + capture {@code L_db}, stash the
     * uncaptured-eligible leftover in-memory, then publish {@code snapshot_info}. Validates the tasks.max
     * range here (only the leader knows the table count). SQL Server holds nothing open, so -- unlike Postgres
     * -- there is no wait-for-followers loop; the leader publishes and exits.
     */
    private void startSmartSnapshotLeader(Configuration config, SqlServerConnectorConfig connectorConfig, int epoch) {
        final String database = connectorConfig.getDatabaseNames().get(0);
        final int numTasks = Integer.parseInt(config.getString(SnapshotCoordinationFacade.NUM_TASKS));
        final boolean shouldStream = connectorConfig.getSnapshotMode() != SqlServerConnectorConfig.SnapshotMode.INITIAL_ONLY;
        final SqlServerSnapshotLifecycleManager lifecycle = new SqlServerSnapshotLifecycleManager(
                connectorConfig, database, () -> SqlServerConnector.connect(connectorConfig));
        this.smartSnapshotLeaderThread = new Thread(
                () -> runSmartSnapshotLeader(config, connectorConfig, database, epoch, numTasks, shouldStream, lifecycle),
                "smart-snapshot-leader");
        this.smartSnapshotLeaderThread.setDaemon(true);
        this.smartSnapshotLeaderThread.start();
    }

    private void runSmartSnapshotLeader(Configuration config, SqlServerConnectorConfig connectorConfig, String database,
                                        int epoch, int numTasks, boolean shouldStream, SqlServerSnapshotLifecycleManager lifecycle) {
        SnapshotCoordinationFacade leaderCoordination = new SnapshotCoordinationFacade(config, connectorConfig);
        try {
            // Leader runs inside task-0; the connector already created the topic, so fail fast if it is absent.
            leaderCoordination.start(MissingTopicPolicy.FAIL);
            if (leaderCoordination.isTaskDone("0", epoch)) {
                LOGGER.info("Smart snapshot: [leader epoch={}] round already complete, skipping preparation", epoch);
                return;
            }
            if (anyRestartNeeded(leaderCoordination, numTasks, epoch)) {
                LOGGER.info("Smart snapshot: [leader epoch={}] restart_needed already present, skipping preparation", epoch);
                return;
            }

            SnapshotSetup setup = lifecycle.prepareSnapshot(shouldStream);

            // Advisory only (the leader is the first place the table count is known): warn if tasks.max is
            // outside the recommended [ceil(t/2), t] band (roughly 1-2 tables per task) but proceed regardless --
            // fewer tasks just means more tables each, more tasks just means some idle with an empty shard.
            int tableCount = setup.tables().size();
            int minTasks = ceilDiv(tableCount, 2);
            if (numTasks < minTasks || numTasks > tableCount) {
                LOGGER.warn("Smart snapshot: [{}] tasks.max={} is outside the recommended range for {} captured "
                        + "table(s); recommended range is [{}, {}] (about 1-2 tables per task) for best snapshot "
                        + "parallelism. Proceeding anyway.", database, numTasks, tableCount, minTasks, tableCount);
            }

            // Stash the uncaptured-eligible leftover BEFORE publishing snapshot_info, so task-0's shard
            // coordinator (which reads it only after seeing snapshot_info) is guaranteed to observe it.
            this.smartSnapshotUncapturedTables = lifecycle.getUncapturedEligibleTables();
            leaderCoordination.writeSnapshotInfo(setup.snapshotName(), setup.consistentPosition(), epoch, setup.tables(), numTasks);
            LOGGER.info("Smart snapshot: [leader epoch={}] published L_db={} numTasks={} for {} table(s)",
                    epoch, setup.consistentPosition(), numTasks, tableCount);
        }
        catch (Throwable t) {
            if (Thread.currentThread().isInterrupted()) {
                LOGGER.info("Smart snapshot: [leader epoch={}] interrupted during preparation (shutdown)", epoch, t);
            }
            else {
                LOGGER.error("Smart snapshot: [leader epoch={}] snapshot preparation failed", epoch, t);
                errorHandler.setProducerThrowable(new DebeziumException("Smart snapshot: [leader epoch=" + epoch + "] snapshot preparation failed", t));
            }
        }
        finally {
            leaderCoordination.stop();
        }
    }

    private static boolean anyRestartNeeded(SnapshotCoordinationFacade coordination, int numTasks, int epoch) {
        for (int i = 0; i < numTasks; i++) {
            if (coordination.isRestartNeeded(String.valueOf(i), epoch)) {
                return true;
            }
        }
        return false;
    }

    private static int ceilDiv(int numerator, int divisor) {
        return (numerator + divisor - 1) / divisor;
    }

    /**
     * Seeds the collapsed/streaming task's offset from the completed round's L_db when it has no committed
     * offset yet. Sharded snapshot tasks carry their own epoch and are excluded via {@code getSmartSnapshotEpoch() == null}.
     */
    private void seedStreamingOffsetsFromCoordination(SqlServerConnectorConfig connectorConfig,
                                                      Offsets<SqlServerPartition, SqlServerOffsetContext> offsets) {
        if (!connectorConfig.isSmartSnapshotEnabled() || connectorConfig.getSmartSnapshotEpoch() != null
                || !SnapshotCoordinationFacade.hasCoordinationBootstrap(connectorConfig.getConfig())) {
            return;
        }
        SnapshotCoordinationFacade facade = SnapshotCoordinationFacade.nonCreating(connectorConfig.getConfig(), connectorConfig);
        try {
            if (!facade.start(MissingTopicPolicy.SKIP)) {
                return; // topic missing/broker unreachable -- fast skip
            }
            // Completion is a separate snapshot_done record (its presence with a consistent_point == round
            // finished); it is NOT a field on snapshot_info. Mirror PostgresConnectorTask#fetchOffsetFromCoordinationTopic.
            Map<String, Object> completionInfo = facade.readCompletion();
            if (completionInfo == null || completionInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT) == null) {
                return;
            }
            Lsn lsn = Lsn.valueOf(String.valueOf(completionInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT)));
            for (Map.Entry<SqlServerPartition, SqlServerOffsetContext> entry : new ArrayList<>(offsets.getOffsets().entrySet())) {
                if (entry.getValue() != null) {
                    continue;
                }
                LOGGER.info("Smart snapshot: [{}] seeding streaming offset from coordination, LSN={}", entry.getKey().getDatabaseName(), lsn);
                SqlServerOffsetContext syntheticOffset = new SqlServerOffsetContext(connectorConfig, TxLogPosition.valueOf(lsn), null, true);
                offsets.getOffsets().put(entry.getKey(), syntheticOffset);
            }
        }
        finally {
            facade.stop();
        }
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        return records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.of(errorHandler);
    }

    @Override
    protected void resetErrorHandlerRetriesIfNeeded(List<SourceRecord> records) {
        // Reset the retries if all partitions have streamed without exceptions at least once after a restart
        if (coordinator.getErrorHandler().getRetries() > 0 && ((SqlServerChangeEventSourceCoordinator) coordinator).firstStreamingIterationCompletedSuccessfully()) {
            coordinator.getErrorHandler().resetRetries();
        }
    }

    @Override
    protected void doStop() {
        if (smartSnapshotLeaderThread != null) {
            smartSnapshotLeaderThread.interrupt();
        }
        if (smartSnapshotCoordination != null) {
            smartSnapshotCoordination.stop();
        }

        try {
            if (dataConnection != null) {
                dataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        try {
            if (metadataConnection != null) {
                metadataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC metadata connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return SqlServerConnectorConfig.ALL_FIELDS;
    }

    private void validateGuardrailLimits(SqlServerConnectorConfig connectorConfig, SqlServerConnection connection) {
        try {
            Set<TableId> allTableIds = connection.getAllTableIds(connectorConfig.getDatabaseNames());
            GuardrailValidator validator = new GuardrailValidator(connectorConfig, schema);
            validator.validate(allTableIds);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to validate guardrail limits", e);
        }
    }

}
