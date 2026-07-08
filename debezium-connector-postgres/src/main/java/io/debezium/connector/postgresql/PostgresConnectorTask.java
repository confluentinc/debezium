/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
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
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresConnection.PostgresValueConverterBuilder;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.GuardrailValidator;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.snapshot.Snapshotter;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;
import io.debezium.util.ThreadNameContext;

/**
 * Kafka connect source task which uses Postgres logical decoding over a streaming replication connection to process DB changes.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectorTask extends BaseSourceTask<PostgresPartition, PostgresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConnectorTask.class);
    private static final String CONTEXT_NAME = "postgres-connector-task";
    private static final int POLL_MS = 10_000;

    private volatile PostgresTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile PostgresConnection jdbcConnection;
    private volatile PostgresConnection beanRegistryJdbcConnection;
    private volatile ReplicationConnection replicationConnection = null;

    private volatile ErrorHandler errorHandler;
    private volatile PostgresSchema schema;
    private volatile SnapshotCoordinationFacade snapshotCoordination;
    // a data snapshotting task in the smart snapshot mode
    private volatile boolean isSmartSnapshotTask;
    // primarily for logging purpose
    private volatile int epoch = -1;
    private volatile String taskId;
    private volatile SmartSnapshotLifecycleManager smartSnapshotLifecycleManager;

    /*
     * This thread manages creation of snapshot and writing the snapshot info to the coordination topic
     * for the tasks to discover the snapshot details and attach to it
     * This involves slot creation or snapshot creation
     * called in start() during the task startup
     */
    private volatile Thread smartSnapshotLeaderThread;

    private Partition.Provider<PostgresPartition> partitionProvider = null;
    private OffsetContext.Loader<PostgresOffsetContext> offsetContextLoader = null;

    private final ReentrantLock commitLock = new ReentrantLock();

    @Override
    public ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> start(Configuration config) {
        final PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(config);
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        ThreadNameContext threadNameContext = ThreadNameContext.from(connectorConfig);

        final Charset databaseCharset;
        try (PostgresConnection tempConnection = new PostgresConnection(connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL,
                threadNameContext)) {
            databaseCharset = tempConnection.getDatabaseCharset();
        }

        final PostgresValueConverterBuilder valueConverterBuilder = (typeRegistry) -> PostgresValueConverter.of(
                connectorConfig,
                databaseCharset,
                typeRegistry);

        MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new PostgresConnection(connectorConfig.getJdbcConfig(), valueConverterBuilder, PostgresConnection.CONNECTION_GENERAL,
                        threadNameContext));
        // Global JDBC connection used both for snapshotting and streaming.
        // Must be able to resolve datatypes.
        jdbcConnection = connectionFactory.mainConnection();
        try {
            jdbcConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        final TypeRegistry typeRegistry = jdbcConnection.getTypeRegistry();
        final PostgresDefaultValueConverter defaultValueConverter = jdbcConnection.getDefaultValueConverter();
        final PostgresValueConverter valueConverter = valueConverterBuilder.build(typeRegistry);

        schema = new PostgresSchema(connectorConfig, defaultValueConverter, topicNamingStrategy, valueConverter);

        isSmartSnapshotTask = isSmartSnapshotTask(config, connectorConfig);

        this.taskContext = isSmartSnapshotTask
                ? new PostgresTaskContext(connectorConfig, connectorConfig.getTaskId(), schema, topicNamingStrategy)
                : new PostgresTaskContext(connectorConfig, schema, topicNamingStrategy);
        this.partitionProvider = new PostgresPartition.Provider(connectorConfig, config);
        this.offsetContextLoader = new PostgresOffsetContext.Loader(connectorConfig);
        final Clock clock = Clock.system();
        Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets = getPreviousOffsets(
                this.partitionProvider, this.offsetContextLoader);
        PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        if (previousOffset == null || previousOffset.isInitialSnapshotRunning()) {
            // A scenario can arise where offset topic contains marker for incompleted snapshot
            // then smart snapshot feature was enabled and the snapshot completed
            // In this particular scenario we should still check the offset topic
            PostgresOffsetContext fromCoordinationTopic = fetchOffsetFromCoordinationTopic(config, connectorConfig, clock);
            if (fromCoordinationTopic != null) {
                // non-null only when smart snapshot actually completed
                previousOffset = fromCoordinationTopic;
                previousOffsets = Offsets.of(previousOffsets.getTheOnlyPartition(), previousOffset);
            }
        }

        // Manual Bean Registration
        beanRegistryJdbcConnection = connectionFactory.newConnection();
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, beanRegistryJdbcConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverter);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);

        // Service providers
        registerServiceProviders(connectorConfig.getServiceRegistry());

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);
        final Snapshotter snapshotter = snapshotterService.getSnapshotter();

        try {
            checkWalLevel(beanRegistryJdbcConnection, snapshotterService);
        }
        catch (SQLException e) {

            LOGGER.error("Failed testing connection for {} with user '{}'", beanRegistryJdbcConnection.connectionString(),
                    beanRegistryJdbcConnection.username(), e);
        }

        // Validate guardrail limits for captured tables to prevent loading excessive table schemas into memory
        if (connectorConfig.getGuardrailCollectionsMax() <= 0) {
            LOGGER.info("Guardrail validation skipped");
        }
        else {
            validateGuardrailLimits(connectorConfig, jdbcConnection);
        }

        validateSchemaHistory(connectorConfig, jdbcConnection::validateLogPosition, previousOffsets, schema, snapshotter);

        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);

        if (previousOffset == null) {
            LOGGER.info("No previous offset found");
        }
        else {
            LOGGER.info("Found previous offset {}", previousOffset);
        }

        try {
            // creation of queue, errorHandler, metadataProvider, signalProcessor, dispatcher, notificationService
            // is moved ahead of slot creation as there is no dependency
            queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            errorHandler = new PostgresErrorHandler(connectorConfig, queue, errorHandler);

            final PostgresEventMetadataProvider metadataProvider = new PostgresEventMetadataProvider();

            SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor = new SignalProcessor<>(
                    PostgresConnector.class, connectorConfig, Map.of(),
                    getAvailableSignalChannels(),
                    DocumentReader.defaultReader(),
                    previousOffsets);

            final PostgresEventDispatcher<TableId> dispatcher = new PostgresEventDispatcher<>(
                    connectorConfig,
                    topicNamingStrategy,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    PostgresChangeRecordEmitter::updateSchema,
                    metadataProvider,
                    connectorConfig.createHeartbeat(
                            topicNamingStrategy,
                            schemaNameAdjuster,
                            () -> new PostgresConnection(connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL, threadNameContext),
                            exception -> {
                                String sqlErrorId = exception.getSQLState();
                                switch (sqlErrorId) {
                                    case "57P01":
                                        // Postgres error admin_shutdown, see https://www.postgresql.org/docs/12/errcodes-appendix.html
                                        throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                                    case "57P03":
                                        // Postgres error cannot_connect_now, see https://www.postgresql.org/docs/12/errcodes-appendix.html
                                        throw new RetriableException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                                    default:
                                        break;
                                }
                            }),
                    schemaNameAdjuster,
                    signalProcessor,
                    connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

            NotificationService<PostgresPartition, PostgresOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                    connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

            if (isSmartSnapshotTask) {
                return startSmartSnapshotTask(
                        config, connectorConfig, connectionFactory, snapshotterService, previousOffsets,
                        dispatcher, notificationService, signalProcessor, metadataProvider, clock, schema);
            }

            SlotState slotInfo = getSlotState(connectorConfig);

            SlotCreationResult slotCreatedInfo = tryToCreateSlot(snapshotter, connectorConfig, slotInfo);

            try {
                jdbcConnection.commit();
            }
            catch (SQLException e) {
                throw new DebeziumException(e);
            }

            ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> coordinator = new PostgresChangeEventSourceCoordinator(
                    previousOffsets,
                    errorHandler,
                    PostgresConnector.class,
                    connectorConfig,
                    new PostgresChangeEventSourceFactory(
                            connectorConfig,
                            snapshotterService,
                            connectionFactory,
                            errorHandler,
                            dispatcher,
                            clock,
                            schema,
                            taskContext,
                            replicationConnection,
                            slotCreatedInfo,
                            slotInfo),
                    new DefaultChangeEventSourceMetricsFactory<>(),
                    dispatcher,
                    schema,
                    snapshotterService,
                    slotInfo,
                    signalProcessor,
                    notificationService);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousContext.restore();
        }
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    private SlotCreationResult tryToCreateSlot(Snapshotter snapshotter, PostgresConnectorConfig connectorConfig, SlotState slotInfo) {

        SlotCreationResult slotCreatedInfo = null;
        if (snapshotter.shouldStream()) {
            replicationConnection = createReplicationConnection(this.taskContext,
                    connectorConfig.maxRetries(), connectorConfig.retryDelay());

            // we need to create the slot before we start streaming if it doesn't exist
            // otherwise we can't stream back changes happening while the snapshot is taking place
            if (slotInfo == null) {
                if (connectorConfig.isReadOnlyConnection()) {
                    LOGGER.warn("Connector is configured to be in read-only mode but replication slot was not found.\n" +
                            "The attempt to create it can fail. Please check you configuration in case.");
                }
                try {
                    slotCreatedInfo = replicationConnection.createReplicationSlot().orElse(null);
                }
                catch (SQLException ex) {
                    String message = "Creation of replication slot failed";
                    if (ex.getMessage().contains("already exists")) {
                        message += "; when setting up multiple connectors for the same database host, please make sure to use a distinct replication slot name for each.";
                    }
                    throw new DebeziumException(message, ex);
                }
            }
        }
        return slotCreatedInfo;
    }

    private SlotState getSlotState(PostgresConnectorConfig connectorConfig) {
        SlotState slotInfo = null;
        try {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(jdbcConnection.serverInfo().toString());
            }
            slotInfo = jdbcConnection.getReplicationSlotState(connectorConfig.slotName(), connectorConfig.plugin().getPostgresPluginName());
        }
        catch (SQLException e) {
            LOGGER.warn("unable to load info of replication slot, Debezium will try to create the slot");
        }
        return slotInfo;
    }

    @FunctionalInterface
    public interface ReplicationConnectionSupplier {
        ReplicationConnection get() throws SQLException;
    }

    public ReplicationConnection createReplicationConnection(PostgresTaskContext taskContext, int maxRetries, Duration retryDelay)
            throws ConnectException {
        return createReplicationConnectionWithRetry(
                () -> taskContext.createReplicationConnection(jdbcConnection), maxRetries, retryDelay);
    }

    // shared retry loop — used by the task's instance method AND the lifecycle
    public static ReplicationConnection createReplicationConnectionWithRetry(
                                                                             ReplicationConnectionSupplier supplier, int maxRetries, Duration retryDelay)
            throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        ReplicationConnection replicationConnection = null;
        while (retryCount <= maxRetries) {
            try {
                return supplier.get();
            }
            catch (SQLException ex) {
                retryCount++;
                if (retryCount > maxRetries) {
                    LOGGER.error("Too many errors connecting to server. All {} retries failed.", maxRetries);
                    throw new ConnectException(ex);
                }

                LOGGER.warn("Error connecting to server; will attempt retry {} of {} after {} " +
                        "seconds. Exception message: {}", retryCount, maxRetries, retryDelay.getSeconds(), ex.getMessage());
                try {
                    metronome.pause();
                }
                catch (InterruptedException e) {
                    LOGGER.warn("Connection retry sleep interrupted by exception: " + e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        return replicationConnection;
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
    protected void doStop() {
        doStopSmartSnapshot();
        // The replication connection is regularly closed at the end of streaming phase
        // in case of error it can happen that the connector is terminated before the stremaing
        // phase is started. It can lead to a leaked connection.
        // This is guard to make sure the connection is closed.
        try {
            if (replicationConnection != null) {
                replicationConnection.close();
            }
        }
        catch (Exception e) {
            LOGGER.trace("Error while closing replication connection", e);
        }

        try {
            if (beanRegistryJdbcConnection != null) {
                beanRegistryJdbcConnection.close();
            }
        }
        catch (Exception e) {
            LOGGER.trace("Error while closing JDBC bean registry connection", e);
        }

        if (jdbcConnection != null) {
            jdbcConnection.close();
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return PostgresConnectorConfig.ALL_FIELDS;
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
        // Do nothing
    }

    @Override
    public void commit() throws InterruptedException {
        if (isSmartSnapshotTask) {
            // In the existing single-task flow (feature disabled), commit() → performCommit()
            // → coordinator.commitOffset() → checks streamingSource != null. During snapshot,
            // streamingSource is null (only created in initStreamEvents() after snapshot completes),
            // so commitOffset() is a no-op during snapshot there too.
            //
            // Smart snapshot tasks never enter streaming — streamingSource stays null permanently.
            // Skipping commit() avoids the unnecessary performCommit() overhead (lock acquisition,
            // offset reading) during snapshot. Per-task offsets still reach connect-offsets via
            // Connect's normal SourceRecord flow.
            return;
        }
        shouldPerformCommit.set(true);
    }

    @Override
    public void performCommit() {
        boolean locked = commitLock.tryLock();

        if (locked) {
            try {
                if (coordinator != null) {
                    Offsets<PostgresPartition, PostgresOffsetContext> offsets = this.getPreviousOffsets(this.partitionProvider, this.offsetContextLoader);
                    if (offsets.getOffsets() != null) {
                        offsets.getOffsets()
                                .entrySet()
                                .stream()
                                .filter(e -> e.getValue() != null)
                                .forEach(entry -> {
                                    Map<String, String> partition = entry.getKey().getSourcePartition();
                                    Map<String, ?> lastOffset = entry.getValue().getOffset();
                                    LOGGER.debug("Committing offset '{}' for partition '{}'", partition, lastOffset);
                                    coordinator.commitOffset(partition, lastOffset);
                                });
                    }
                }
            }
            finally {
                commitLock.unlock();
            }
        }
        else {
            LOGGER.warn("Couldn't commit processed log positions with the source database due to a concurrent connector shutdown or restart");
        }
    }

    public PostgresTaskContext getTaskContext() {
        return taskContext;
    }

    private void validateGuardrailLimits(PostgresConnectorConfig connectorConfig, PostgresConnection connection) {
        try {
            Set<TableId> allTableIds = connection.getAllTableIds(connectorConfig.databaseName());
            GuardrailValidator validator = new GuardrailValidator(connectorConfig, schema);
            validator.validate(allTableIds);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to validate guardrail limits", e);
        }
    }

    private static void checkWalLevel(PostgresConnection connection, SnapshotterService snapshotterService) throws SQLException {
        final String walLevel = connection.queryAndMap(
                "SHOW wal_level",
                connection.singleResultMapper(rs -> rs.getString("wal_level"), "Could not fetch wal_level"));
        if (!"logical".equals(walLevel)) {

            if (snapshotterService.getSnapshotter() != null && snapshotterService.getSnapshotter().shouldStream()) {
                // Logical WAL_LEVEL is only necessary for CDC snapshotting
                throw new SQLException("Postgres server wal_level property must be 'logical' but is: '" + walLevel + "'");
            }
            else {
                LOGGER.warn("WAL_LEVEL check failed but this is ignored as CDC was not requested");
            }
        }
    }

    private boolean isSmartSnapshotTask(Configuration config, CommonConnectorConfig connectorConfig) {
        String numTasksStr = config.getString(SnapshotCoordinationFacade.NUM_TASKS);
        boolean isParallelRound = numTasksStr != null && !"1".equals(numTasksStr);

        // if the feature is enabled and taskId is null in ideal scenario the task should be streaming
        // if it is a data snapshot task with feature enabled, it continues with single task data snapshot
        return connectorConfig.isSmartSnapshotEnabled() && connectorConfig.getTaskId() != null && isParallelRound;
    }

    private ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> startSmartSnapshotTask(
                                                                                                          Configuration config,
                                                                                                          PostgresConnectorConfig connectorConfig,
                                                                                                          MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory,
                                                                                                          SnapshotterService snapshotterService,
                                                                                                          Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                                                                                          PostgresEventDispatcher<TableId> dispatcher,
                                                                                                          NotificationService<PostgresPartition, PostgresOffsetContext> notificationService,
                                                                                                          SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor,
                                                                                                          PostgresEventMetadataProvider metadataProvider, Clock clock,
                                                                                                          PostgresSchema schema) {
        final String taskId = connectorConfig.getTaskId();
        if (config.getString(SnapshotCoordinationFacade.EPOCH) == null || config.getString(SnapshotCoordinationFacade.NUM_TASKS) == null) {
            // if taskId is null, we would never enter this branch
            throw new DebeziumException(
                    String.format("Smart snapshot: [role=task taskId=%s] Failing as required configs [epoch, num_tasks] are missing.", taskId));
        }

        this.epoch = Integer.parseInt(config.getString(SnapshotCoordinationFacade.EPOCH));
        this.taskId = taskId;
        LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Starting task", taskId, epoch);

        // todo should the leader thread creates its own coordination this would solve the gate required in start to make it idempotent
        this.snapshotCoordination = new SnapshotCoordinationFacade(config, connectorConfig);
        try {
            // end the setup txn (guardrail query, etc.) so the snapshot's SET is the first
            jdbcConnection.commit();
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        // task-0 is the leader: discover tables, prepare the snapshot (slot/export + lock-all) on a background thread.
        if ("0".equals(taskId)) {
            final int leaderEpoch = epoch;
            final boolean shouldStream = !PostgresConnectorConfig.SnapshotMode.INITIAL_ONLY.getValue()
                    .equals(connectorConfig.getSnapshotMode().getValue());
            final int numTasks = Integer.parseInt(config.getString(SnapshotCoordinationFacade.NUM_TASKS));
            final PostgresSmartSnapshotLifecycleManager lifecycle = new PostgresSmartSnapshotLifecycleManager(
                    connectorConfig, connectionFactory, taskContext, snapshotterService,
                    schema, dispatcher, notificationService, clock, leaderEpoch);
            this.smartSnapshotLifecycleManager = lifecycle;

            // only used for logging
            final PostgresPartition leaderPartition = new PostgresPartition(connectorConfig.getConnectorName(), "", "0");
            SnapshotCoordinationFacade leaderSnapshotCoordination = new SnapshotCoordinationFacade(config, connectorConfig);
            this.smartSnapshotLeaderThread = new Thread(
                    new SmartSnapshotLeader(
                            lifecycle, leaderSnapshotCoordination, this.errorHandler,
                            leaderEpoch, numTasks, shouldStream, POLL_MS,
                            () -> taskContext.configureLoggingContext("smart-snapshot-leader", leaderPartition)),
                    "smart-snapshot-leader");
            this.smartSnapshotLeaderThread.setDaemon(true);
            this.smartSnapshotLeaderThread.start();
        }

        // The leader task background thread handles slot creation & replication connection creation, skip those
        coordinator = new PostgresSmartSnapshotChangeEventSourceCoordinator(
                previousOffsets, errorHandler, PostgresConnector.class, connectorConfig,
                new PostgresChangeEventSourceFactory(connectorConfig, snapshotterService,
                        connectionFactory, errorHandler, dispatcher, clock, schema, taskContext,
                        null /* replicationConnection */,
                        null /* slotCreatedInfo */,
                        null /* slotInfo */),
                new DefaultChangeEventSourceMetricsFactory<>(),
                dispatcher, schema, snapshotterService,
                null /* slotInfo */,
                signalProcessor,
                notificationService,
                epoch, snapshotCoordination, connectorConfig.getTaskId());

        coordinator.start(taskContext, this.queue, metadataProvider);
        return coordinator;
    }

    /**
     * Runs on task-0 (the leader) on a background thread: prepares the shared snapshot (slot create / export
     * + lock), publishes it on the coordination topic, then waits until every task has imported it -- or one
     * signals a restart -- before releasing the held connections. Extracted from an inline Runnable so the
     * orchestration can be unit tested with a mock lifecycle + coordination (pollMs = 0 skips the real wait).
     */
    static class SmartSnapshotLeader implements Runnable {

        private final SmartSnapshotLifecycleManager lifecycle;
        private final SnapshotCoordinationFacade leaderSnapshotCoordination;
        private final ErrorHandler errorHandler;
        private final int leaderEpoch;
        private final int numTasks;
        private final boolean shouldStream;
        private final long pollMs;
        private final Runnable loggingContextSetup;

        SmartSnapshotLeader(SmartSnapshotLifecycleManager lifecycle, SnapshotCoordinationFacade leaderSnapshotCoordination,
                            ErrorHandler errorHandler, int leaderEpoch, int numTasks, boolean shouldStream, long pollMs,
                            Runnable loggingContextSetup) {
            this.lifecycle = lifecycle;
            this.leaderSnapshotCoordination = leaderSnapshotCoordination;
            this.errorHandler = errorHandler;
            this.leaderEpoch = leaderEpoch;
            this.numTasks = numTasks;
            this.shouldStream = shouldStream;
            this.pollMs = pollMs;
            this.loggingContextSetup = loggingContextSetup;
        }

        @Override
        public void run() {
            try {
                loggingContextSetup.run();

                // background thread — safe to block on the topic read here
                leaderSnapshotCoordination.start();

                // a completed task-0 that got restarted must NOT re-prepare, if other task can't finish the coordinator would start a new round
                if (leaderSnapshotCoordination.isDone("0", leaderEpoch)) {
                    LOGGER.info("Smart snapshot: [role=leader epoch={}] Snapshot already completed, skipping leader preparation", leaderEpoch);
                    // thread ends; no re-export, no re-lock, {server} key untouched. Foreground idles until downscale.
                    return;
                }

                // mid-prep restart already flagged
                if (anyRestartNeeded()) {
                    LOGGER.info("Smart snapshot: [role=leader epoch={}] Detected restart_needed marker, skipping snapshot preparation", leaderEpoch);
                    return;
                }

                final SmartSnapshotLifecycleManager.SnapshotSetup setup = lifecycle.prepareSnapshot(shouldStream);

                // todo list of tables might require compression or enable compression on the coordination topic
                leaderSnapshotCoordination.writeSnapshotInfo(setup.snapshotName(), setup.consistentPosition(), leaderEpoch, setup.tables(), numTasks);

                LOGGER.info("Smart snapshot: [role=leader epoch={}] Prepared snapshot={}, LSN={}",
                        leaderEpoch, setup.snapshotName(), setup.consistentPosition());

                // wait until every task has imported the snapshot and (optionally) locked its subset, then release and end the thread
                while (!Thread.currentThread().isInterrupted() && !allTasksStartedTransaction() && !anyRestartNeeded()) {
                    Thread.sleep(pollMs);
                    lifecycle.keepAlive();
                }
                if (allTasksStartedTransaction()) {
                    // releaseSnapshot(), slot persists; thread ends
                    lifecycle.onAllTasksStartedTransaction();
                }
                else if (anyRestartNeeded()) {
                    LOGGER.warn("Smart snapshot: [role=leader epoch={}] Detected `restart_needed` marker, releasing early", leaderEpoch);
                    // early abort: the connector monitor bumps epoch + reconfigures
                    lifecycle.releaseSnapshot();
                }
                LOGGER.info("Smart snapshot: [role=leader epoch={}] All tasks have started their transaction, stopping the leader thread", leaderEpoch);
            }
            catch (InterruptedException e) {
                // Path A: an interrupt-aware wait (Thread.sleep in the keep-alive loop) was interrupted
                // by the task-stop path. The flag was cleared when InterruptedException was thrown, so
                // restore it and end the thread.
                LOGGER.info("Smart snapshot: [role=leader epoch={}] Interrupted while waiting, stopping snapshot preparation", leaderEpoch);
                Thread.currentThread().interrupt();
                // todo verify the behaviour
            }
            catch (Throwable throwable) {
                // todo verify the behaviour
                // Catching Throwable ensures that an Error (for example NoClassDefFoundError or OutOfMemoryError)
                // also fails the task, instead of the
                // thread dying silently and leaving the snapshot round stranded.
                lifecycle.releaseSnapshot();
                if (Thread.currentThread().isInterrupted()) {
                    // Path B: the thread was blocked in a call that ignores interrupts (a JDBC call), so
                    // the task-stop path aborted it by closing the connection. The exception here is that
                    // abort (for example a SQLException), not an InterruptedException, but the interrupt
                    // flag is still set, which tells us this is a shutdown rather than a real failure.
                    LOGGER.error("Smart snapshot: [role=leader epoch={}] Snapshot preparation aborted by shutdown, held connection closed", leaderEpoch, throwable);
                    return;
                }
                LOGGER.error("Smart snapshot: [role=leader epoch={}] Snapshot preparation failed", leaderEpoch, throwable);
                // Fail the task with the real error. We do NOT write restart_needed here: preparation failed,
                // so the snapshot was never published and there is nothing to throw away. When task-0
                // restarts it sees its own marker and writes restart_needed then,
                // which cause connector to bump the epoch.
                errorHandler
                        .setProducerThrowable(new DebeziumException("Smart snapshot: [role=leader epoch=" + leaderEpoch + "] Snapshot preparation failed", throwable));
            }
            finally {
                boolean wasInterrupted = Thread.interrupted();
                try {
                    LOGGER.info("Smart snapshot: [role=leader epoch={}] Cleaning up snapshot coordination resources", leaderEpoch);
                    // this is leader's private kafka based SnapshotCoordination
                    leaderSnapshotCoordination.stop();
                }
                catch (Exception e) {
                    LOGGER.warn("Smart snapshot: [role=leader epoch={}] Non-critical failure shutting down coordination log components. error={}", leaderEpoch,
                            e.getMessage());
                }
                if (wasInterrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        boolean allTasksStartedTransaction() {
            for (int i = 0; i < numTasks; i++) {
                if (!leaderSnapshotCoordination.isTransactionStarted(String.valueOf(i), leaderEpoch)) {
                    return false;
                }
            }
            return true;
        }

        boolean anyRestartNeeded() {
            for (int i = 0; i < numTasks; i++) {
                if (leaderSnapshotCoordination.isRestartNeeded(String.valueOf(i), leaderEpoch)) {
                    return true;
                }
            }
            return false;
        }
    }

    private PostgresOffsetContext fetchOffsetFromCoordinationTopic(
                                                                   Configuration config, PostgresConnectorConfig connectorConfig,
                                                                   Clock clock) {
        // Post-downscale streaming task: read LSN from coordination topic only if the feature is still enabled
        // Otherwise, the snapshot taken in the smart snapshot mode is discarded
        if (!connectorConfig.isSmartSnapshotEnabled() || isSmartSnapshotTask) {
            return null;
        }

        if (!SnapshotCoordinationFacade.hasCoordinationBootstrap(config, connectorConfig)) {
            return null;
        }

        SnapshotCoordinationFacade facade = SnapshotCoordinationFacade.readOnly(config, connectorConfig);

        try {
            if (!facade.startForRead()) {
                // topic doesn't exist / broker unreachable skip fast
                return null;
            }

            Map<String, Object> coordinationData = facade.readSnapshotInfo();

            if (coordinationData != null
                    && Boolean.TRUE.equals(coordinationData.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY))
                    && coordinationData.get(SnapshotCoordinationFacade.CONSISTENT_POINT) != null) {
                String lsnStr = String.valueOf(coordinationData.get(SnapshotCoordinationFacade.CONSISTENT_POINT));
                Lsn lsn = Lsn.valueOf(lsnStr);
                LOGGER.info("Smart snapshot: [role=task] Post-downscale streaming task, using LSN={} from coordination topic", lsn);
                // Create synthetic offset — snapshot completed, start streaming from this LSN
                PostgresOffsetContext syntheticOffset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, clock);
                syntheticOffset.updateWalPosition(
                        lsn, null,
                        clock.currentTimeAsInstant(),
                        null, null, null, null);
                syntheticOffset.postSnapshotCompletion();
                return syntheticOffset;
            }
            return null;
        }
        finally {
            facade.stop();
        }
    }

    private void doStopSmartSnapshot() {
        if (!isSmartSnapshotTask) {
            return;
        }
        stopSmartSnapshot(smartSnapshotLeaderThread, smartSnapshotLifecycleManager, snapshotCoordination, 10_000, taskId, epoch);
        smartSnapshotLeaderThread = null;
        smartSnapshotLifecycleManager = null;
    }

    /**
     * Stops the smart snapshot related stuff for this task. This runs on the Kafka Connect task-stop
     * thread, which is a different thread from the leader thread.
     * <p>
     * Only smart snapshot tasks set up any of these resources, so for every other task this method
     * returns early. Even among smart snapshot tasks, the prep thread and lifecycle manager exist
     * only on the leader (task-0); followers have just the coordination facade.
     * <p>
     * The steps must run in this order:
     * 1. interrupt() wakes the prep thread if it is sleeping in the keep-alive loop.
     * 2. releaseSnapshot() closes the held connections. If the prep thread is waiting on a
     * database call that cannot be interrupted, closing the connection aborts that call so the
     * thread can finish. interrupt() is done first so that the error raised by the aborted
     * call is recognised as a shutdown rather than a real failure.
     * 3. join() waits for the prep thread to actually finish, so that it is no longer using the
     * coordination facade when we stop it in the next step. The wait is bounded so that stop
     * can never block forever.
     * 4. stop() closes the coordination facade last, because it wraps a Kafka client that is not
     * safe to use on one thread and close on another at the same time.
     */
    static void stopSmartSnapshot(Thread leaderThread, SmartSnapshotLifecycleManager lifecycle,
                                  SnapshotCoordinationFacade coordinationFacade, long joinMs, String taskId, int epoch) {
        // 1. Signal the leader thread to stop and unblock it wherever it may be waiting.
        // interrupt() wakes it from sleep(); releaseSnapshot() closes and aborts the held
        // connections, ending any query it is waiting on.
        if (leaderThread != null) {
            LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Stopping snapshot preparation and releasing held connections", taskId, epoch);
            leaderThread.interrupt();
        }
        if (lifecycle != null) {
            lifecycle.releaseSnapshot();
        }

        boolean currentThreadWasInterrupted = false;

        // 2. Wait for the prep thread to finish, so it is no longer using the coordination
        // facade when we close it below. Bounded so stop can never block forever.
        if (leaderThread != null) {
            try {
                leaderThread.join(joinMs);
                if (leaderThread.isAlive()) {
                    LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Leader thread did not stop within {} ms", taskId, epoch, joinMs);
                }
            }
            catch (InterruptedException e) {
                LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Task thread was interrupted while waiting for leader thread join", taskId, epoch);
                currentThreadWasInterrupted = true;
            }
        }
        if (coordinationFacade != null) {
            try {
                LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Stopping coordination facade", taskId, epoch);
                coordinationFacade.stop();
            }
            catch (Exception e) {
                LOGGER.error("Smart snapshot: [role=task taskId={} epoch={}] Failed to cleanly close coordination facade log. error={}", taskId, epoch, e.getMessage());
            }
        }

        if (currentThreadWasInterrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
