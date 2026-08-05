/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogEventMetadataProvider;
import io.debezium.connector.binlog.BinlogSourceTask;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.mysql.jdbc.MySqlConnection;
import io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration;
import io.debezium.connector.mysql.jdbc.MySqlFieldReaderResolver;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.GuardrailValidator;
import io.debezium.pipeline.metrics.TaskStateMetrics;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLeader;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.snapshot.Snapshotter;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.ThreadNameContext;

/**
 * The main task executing streaming from MySQL.
 * Responsible for lifecycle management of the streaming code.
 *
 * @author Jiri Pechanec
 *
 */
public class MySqlConnectorTask extends BinlogSourceTask<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnectorTask.class);
    private static final String CONTEXT_NAME = "mysql-connector-task";

    private volatile MySqlTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile BinlogConnectorConnection connection;
    private volatile BinlogConnectorConnection beanRegistryJdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile MySqlDatabaseSchema schema;

    // Smart snapshot state (only set when this is a smart snapshot data task).
    private volatile SnapshotCoordinationFacade snapshotCoordination;
    private volatile boolean isSmartSnapshotTask;
    private volatile int epoch = -1;
    private volatile String taskId;
    private volatile SmartSnapshotLifecycleManager smartSnapshotLifecycleManager;
    // The leader (task-0) background thread that prepares the shared snapshot and publishes it.
    private volatile Thread smartSnapshotLeaderThread;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<MySqlPartition, MySqlOffsetContext> start(Configuration configuration) {
        final Clock clock = Clock.system();
        final MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(configuration);
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(MySqlConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        final MySqlValueConverters valueConverters = getValueConverters(connectorConfig);

        // DBZ-3238: automatically set "useCursorFetch" to true when a snapshot fetch size other than the default of -1 is given
        // By default do not load whole result sets into memory
        final Configuration config = configuration.edit()
                .withDefault("database.responseBuffering", "adaptive")
                .withDefault("database.fetchSize", 10_000)
                .withDefault("database.useCursorFetch", connectorConfig.useCursorFetch())
                .build();

        MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(() -> {
            final MySqlConnectionConfiguration connectionConfig = new MySqlConnectionConfiguration(config);
            return new MySqlConnection(connectionConfig, MySqlFieldReaderResolver.resolve(connectorConfig), ThreadNameContext.from(connectorConfig));
        });

        connection = connectionFactory.mainConnection();

        Offsets<MySqlPartition, MySqlOffsetContext> previousOffsets = getPreviousOffsets(
                new MySqlPartition.Provider(connectorConfig, config),
                new MySqlOffsetContext.Loader(connectorConfig));

        this.isSmartSnapshotTask = isSmartSnapshotTask(config, connectorConfig);

        // Post-downscale streaming task: no Kafka-Connect offset yet, so seed it from the coordination topic
        // (snapshot completed, positioned at P) so streaming resumes from where the parallel snapshot ended.
        if (connectorConfig.isSmartSnapshotEnabled() && previousOffsets.getTheOnlyOffset() == null) {
            MySqlOffsetContext fromCoordinationTopic = SnapshotCoordinationFacade.fetchOffsetFromCoordinationTopic(
                    config, connectorConfig, isSmartSnapshotTask, buildOffsetFromConsistentPoint(connectorConfig));
            if (fromCoordinationTopic != null) {
                previousOffsets = Offsets.of(previousOffsets.getTheOnlyPartition(), fromCoordinationTopic);
            }
        }

        final boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();
        this.schema = new MySqlDatabaseSchema(connectorConfig, valueConverters, topicNamingStrategy, schemaNameAdjuster, tableIdCaseInsensitive);
        taskContext = new MySqlTaskContext(connectorConfig, schema);

        // Manual Bean Registration
        beanRegistryJdbcConnection = connectionFactory.newConnection();
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, beanRegistryJdbcConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverters);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);

        // Service providers
        registerServiceProviders(connectorConfig.getServiceRegistry());

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);
        final Snapshotter snapshotter = snapshotterService.getSnapshotter();

        validateBinlogConfiguration(snapshotter, connection);

        // If the binlog position is not available it is necessary to re-execute snapshot
        if (validateSnapshotFeasibility(snapshotter, previousOffsets.getTheOnlyOffset(), connection)) {
            previousOffsets.resetOffset(previousOffsets.getTheOnlyPartition());
        }

        // Validate guardrail limits for captured tables to prevent loading excessive table schemas into memory
        if (connectorConfig.getGuardrailCollectionsMax() <= 0) {
            LOGGER.info("Guardrail validation skipped");
        }
        else {
            validateGuardrailLimits(connectorConfig, connection);
        }

        LOGGER.info("Closing connection before starting schema recovery");

        try {
            connection.close();
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        MySqlOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        validateSchemaHistory(connectorConfig, connection::validateLogPosition, previousOffsets, schema, snapshotter);

        LOGGER.info("Reconnecting after validating schema recovery");

        try {
            try {
                connection.execute("SELECT 1");
            }
            catch (SQLException e) {
                LOGGER.warn("Connection was dropped during schema recovery. Reconnecting...");
                try {
                    connection.close();
                }
                catch (Exception e1) {
                    // Ignore any error
                }
                connection = connectionFactory.mainConnection();
            }

            connection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to reconnect after schema recovery", e);
        }

        // If the binlog position is not available it is necessary to re-execute snapshot
        if (previousOffset == null) {
            LOGGER.info("No previous offset found");
        }
        else {
            LOGGER.info("Found previous offset {}", previousOffset);
        }

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .buffering()
                .build();

        errorHandler = new MySqlErrorHandler(connectorConfig, queue, errorHandler);

        final BinlogEventMetadataProvider metadataProvider = new BinlogEventMetadataProvider();

        SignalProcessor<MySqlPartition, MySqlOffsetContext> signalProcessor = new SignalProcessor<>(
                MySqlConnector.class, connectorConfig, Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsets);

        final Configuration heartbeatConfig = config;
        final EventDispatcher<MySqlPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                null,
                metadataProvider,
                connectorConfig.createHeartbeat(
                        topicNamingStrategy,
                        schemaNameAdjuster,
                        () -> new MySqlConnection(
                                new MySqlConnectionConfiguration(heartbeatConfig),
                                MySqlFieldReaderResolver.resolve(connectorConfig), ThreadNameContext.from(connectorConfig)),
                        new BinlogHeartbeatErrorHandler()),
                schemaNameAdjuster, signalProcessor, connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

        final MySqlStreamingChangeEventSourceMetrics streamingMetrics = new MySqlStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider);

        NotificationService<MySqlPartition, MySqlOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        ChangeEventSourceCoordinator<MySqlPartition, MySqlOffsetContext> coordinator;
        if (isSmartSnapshotTask) {
            coordinator = startSmartSnapshotTask(config, connectorConfig, connectionFactory, snapshotterService,
                    previousOffsets, dispatcher, notificationService, signalProcessor, metadataProvider, clock,
                    schema, streamingMetrics);
        }
        else {
            coordinator = new ChangeEventSourceCoordinator<>(
                    previousOffsets,
                    errorHandler,
                    MySqlConnector.class,
                    connectorConfig,
                    new MySqlChangeEventSourceFactory(
                            connectorConfig,
                            connectionFactory,
                            errorHandler,
                            dispatcher,
                            clock,
                            schema,
                            taskContext,
                            streamingMetrics,
                            queue,
                            snapshotterService),
                    new MySqlChangeEventSourceMetricsFactory(streamingMetrics),
                    dispatcher,
                    schema,
                    signalProcessor,
                    notificationService,
                    snapshotterService);
        }

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    private boolean isSmartSnapshotTask(Configuration config, MySqlConnectorConfig connectorConfig) {
        String numTasksStr = config.getString(SnapshotCoordinationFacade.NUM_TASKS);
        boolean isParallelRound = numTasksStr != null && !"1".equals(numTasksStr);
        // If the feature is on and taskId is null this is (or will downscale to) the streaming task; only a
        // data-snapshot task in a parallel round is a smart snapshot task.
        return connectorConfig.isSmartSnapshotEnabled() && connectorConfig.getTaskId() != null && isParallelRound;
    }

    /**
     * Builds a synthetic completed offset at P for the post-downscale streaming task, decoding the
     * coordination topic's {@code file:pos:gtids} consistent point.
     */
    private Function<String, MySqlOffsetContext> buildOffsetFromConsistentPoint(MySqlConnectorConfig connectorConfig) {
        return cp -> {
            String[] p = cp.split(":", 3);
            String file = p[0];
            long pos = Long.parseLong(p[1]);
            String gtids = (p.length > 2 && !p[2].isEmpty()) ? p[2] : null;
            MySqlOffsetContext offset = new MySqlOffsetContext(
                    null, true, new TransactionContext(),
                    connectorConfig.isReadOnlyConnection()
                            ? new MySqlReadOnlyIncrementalSnapshotContext<>()
                            : new SignalBasedIncrementalSnapshotContext<>(),
                    new SourceInfo(connectorConfig)); // snapshotCompleted=true -> ctor calls postSnapshotCompletion()
            offset.setBinlogStartPoint(file, pos);
            offset.setCompletedGtidSet(gtids);
            LOGGER.info("Smart snapshot: [role=task] Post-downscale streaming task, seeding from P=({}:{}) gtid={}", file, pos, gtids);
            return offset;
        };
    }

    private ChangeEventSourceCoordinator<MySqlPartition, MySqlOffsetContext> startSmartSnapshotTask(
                                                                                                    Configuration config,
                                                                                                    MySqlConnectorConfig connectorConfig,
                                                                                                    MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
                                                                                                    SnapshotterService snapshotterService,
                                                                                                    Offsets<MySqlPartition, MySqlOffsetContext> previousOffsets,
                                                                                                    EventDispatcher<MySqlPartition, TableId> dispatcher,
                                                                                                    NotificationService<MySqlPartition, MySqlOffsetContext> notificationService,
                                                                                                    SignalProcessor<MySqlPartition, MySqlOffsetContext> signalProcessor,
                                                                                                    BinlogEventMetadataProvider metadataProvider,
                                                                                                    Clock clock,
                                                                                                    MySqlDatabaseSchema schema,
                                                                                                    MySqlStreamingChangeEventSourceMetrics streamingMetrics) {
        this.taskId = connectorConfig.getTaskId();
        if (config.getString(SnapshotCoordinationFacade.EPOCH) == null || config.getString(SnapshotCoordinationFacade.NUM_TASKS) == null) {
            throw new DebeziumException(String.format(
                    "Smart snapshot: [role=task taskId=%s] Failing as required configs [epoch, num_tasks] are missing.", taskId));
        }
        this.epoch = Integer.parseInt(config.getString(SnapshotCoordinationFacade.EPOCH));
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Starting task", taskId, epoch);

        this.snapshotCoordination = SnapshotCoordinationFacade.nonCreating(config, connectorConfig);

        // task-0 is the leader: on a background thread lock, capture P, write the full schema history, publish.
        if ("0".equals(taskId)) {
            final int leaderEpoch = epoch;
            final boolean shouldStream = !BinlogConnectorConfig.SnapshotMode.INITIAL_ONLY.getValue()
                    .equals(connectorConfig.getSnapshotMode().getValue());
            final int numTasks = Integer.parseInt(config.getString(SnapshotCoordinationFacade.NUM_TASKS));
            final MySqlSnapshotChangeEventSourceMetrics leaderMetrics = new MySqlSnapshotChangeEventSourceMetrics(
                    taskContext, queue, metadataProvider, new TaskStateMetrics(taskContext));
            final MySqlSmartSnapshotLifecycleManager lifecycle = new MySqlSmartSnapshotLifecycleManager(
                    connectorConfig, connectionFactory, schema, dispatcher, clock, leaderMetrics,
                    notificationService, snapshotterService, leaderEpoch);
            this.smartSnapshotLifecycleManager = lifecycle;

            final SnapshotCoordinationFacade leaderCoordination = SnapshotCoordinationFacade.nonCreating(config, connectorConfig);
            this.smartSnapshotLeaderThread = new Thread(
                    new SmartSnapshotLeader(lifecycle, leaderCoordination, this.errorHandler,
                            leaderEpoch, numTasks, shouldStream, connectorConfig,
                            () -> taskContext.configureLoggingContext("smart-snapshot-leader")),
                    "smart-snapshot-leader");
            this.smartSnapshotLeaderThread.setDaemon(true);
            this.smartSnapshotLeaderThread.start();
        }

        return new MySqlSmartSnapshotChangeEventSourceCoordinator(
                previousOffsets, errorHandler, MySqlConnector.class, connectorConfig,
                new MySqlChangeEventSourceFactory(connectorConfig, connectionFactory, errorHandler, dispatcher,
                        clock, schema, taskContext, streamingMetrics, queue, snapshotterService),
                new MySqlChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher, schema, snapshotterService, signalProcessor, notificationService,
                epoch, snapshotCoordination, taskId);
    }

    private MySqlValueConverters getValueConverters(MySqlConnectorConfig configuration) {
        return new MySqlValueConverters(
                configuration.getDecimalMode(),
                configuration.getTemporalPrecisionMode(),
                configuration.getBigIntUnsignedHandlingMode().asBigIntUnsignedMode(),
                configuration.binaryHandlingMode(),
                configuration.isTimeAdjustedEnabled() ? MySqlValueConverters::adjustTemporal : x -> x,
                configuration.getEventConvertingFailureHandlingMode(),
                configuration.getServiceRegistry());
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();
        return records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.of(errorHandler);
    }

    @Override
    protected void doStop() {
        if (isSmartSnapshotTask) {
            SmartSnapshotLeader.stopSmartSnapshot(
                    smartSnapshotLeaderThread, smartSnapshotLifecycleManager,
                    snapshotCoordination, 10_000, taskId, epoch);
            smartSnapshotLeaderThread = null;
            smartSnapshotLifecycleManager = null;
        }

        shutdownQueue(queue);

        try {
            if (connection != null) {
                connection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        try {
            if (beanRegistryJdbcConnection != null) {
                beanRegistryJdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC bean registry connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return MySqlConnectorConfig.ALL_FIELDS;
    }

    private void validateGuardrailLimits(MySqlConnectorConfig connectorConfig, BinlogConnectorConnection connection) {
        try {
            Set<TableId> allTableIds = connection.getAllTableIds();
            GuardrailValidator validator = new GuardrailValidator(connectorConfig, schema);
            validator.validate(allTableIds);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to validate guardrail limits", e);
        }
    }

}
