/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
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
import io.debezium.pipeline.source.snapshot.KafkaLogSnapshotCoordination;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.source.snapshot.SnapshotLifecycleManager;
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
import io.debezium.util.Collect;
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

    private volatile PostgresTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile PostgresConnection jdbcConnection;
    private volatile PostgresConnection beanRegistryJdbcConnection;
    private volatile ReplicationConnection replicationConnection = null;

    private volatile ErrorHandler errorHandler;
    private volatile PostgresSchema schema;
    private volatile SnapshotCoordination snapshotCoordination;
    private volatile boolean isSmartSnapshotTask;
    private volatile SnapshotLifecycleManager smartLifecycleManager;

    /*
     * This thread manages creation of snapshot and writing the snapshot info to the coordination topic
     * for the tasks to discover the snapshot details and attach to it
     * This involves slot creation or snapshot creation
     * called in start() during the task startup
     */
    private volatile Thread smartSnapshotPreparationThread;

    /*
     * Stores any error raised in the preparation thread
     */
    private volatile Throwable smartSnapshotPreparationError;

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

        isSmartSnapshotTask = connectorConfig.isSmartSnapshotEnabled() && connectorConfig.getTaskId() != null;

        this.taskContext = isSmartSnapshotTask
                ? new PostgresTaskContext(connectorConfig, connectorConfig.getTaskId(), schema, topicNamingStrategy)
                : new PostgresTaskContext(connectorConfig, schema, topicNamingStrategy);
        this.partitionProvider = new PostgresPartition.Provider(connectorConfig, config);
        this.offsetContextLoader = new PostgresOffsetContext.Loader(connectorConfig);
        final Clock clock = Clock.system();
        Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets = getPreviousOffsets(
                this.partitionProvider, this.offsetContextLoader);
        PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        if (previousOffset == null) {
            previousOffset = fetchOffsetFromCoordinationTopic(config, previousOffsets.getTheOnlyPartition(), connectorConfig, clock);
            if (previousOffset != null) {
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
                        dispatcher, notificationService, signalProcessor, metadataProvider, clock);
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
            replicationConnection = taskContext.createReplicationConnectionWithRetry(jdbcConnection,
                    connectorConfig.dropSlotOnStop());

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

    // shared retry loop — used by the task's instance method AND the lifecycle
    public static ReplicationConnection createReplicationConnectionWithRetry(
                                                                             ReplicationConnectionSupplier supplier, int maxRetries, Duration retryDelay)
            throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
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
                LOGGER.warn("Error connecting to server; retry {} of {} after {}s: {}",
                        retryCount, maxRetries, retryDelay.getSeconds(), ex.getMessage());
                try {
                    metronome.pause();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new ConnectException("Failed to create replication connection");
    }

    public ReplicationConnection createReplicationConnection(PostgresTaskContext taskContext, int maxRetries, Duration retryDelay)
            throws ConnectException {
        return createReplicationConnectionWithRetry(
                () -> taskContext.createReplicationConnection(jdbcConnection), maxRetries, retryDelay);
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

    private ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> startSmartSnapshotTask(
                                                                                                          Configuration config,
                                                                                                          PostgresConnectorConfig connectorConfig,
                                                                                                          MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory,
                                                                                                          SnapshotterService snapshotterService,
                                                                                                          Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                                                                                          PostgresEventDispatcher<TableId> dispatcher,
                                                                                                          NotificationService<PostgresPartition, PostgresOffsetContext> notificationService,
                                                                                                          SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor,
                                                                                                          PostgresEventMetadataProvider metadataProvider, Clock clock) {
        int epoch = Integer.parseInt(config.getString(SmartSnapshotConnectorCoordinator.EPOCH_KEY, "1"));
        LOGGER.info("Smart snapshot: task-{} epoch={}", connectorConfig.getTaskId(), epoch);

        // Create coordination + coordinator
        String coordinationTopic = connectorConfig.getLogicalName() + ".snapshot-coordination";
        String clientIdSuffix = connectorConfig.getLogicalName() + "-coordination-task-" + connectorConfig.getTaskId();
        Map<String, Object> clientConfig = KafkaLogSnapshotCoordination.clientConfigFromOverrides(
                config, connectorConfig.getSmartSnapshotCoordinationBootstrapServers());
        this.snapshotCoordination = new KafkaLogSnapshotCoordination(clientConfig, coordinationTopic, clientIdSuffix);

        // task-0 is the leader: prepare the snapshot (slot/export + lock-all) on a background thread.
        if ("0".equals(connectorConfig.getTaskId())) {
            final int leaderEpoch = epoch;
            final List<TableId> allTables = parseTableList(config.getString(SmartSnapshotConnectorCoordinator.ALL_TABLES_KEY));
            final boolean shouldStream = !PostgresConnectorConfig.SnapshotMode.INITIAL_ONLY.getValue()
                    .equals(connectorConfig.getSnapshotMode().getValue());
            final PostgresSnapshotLifecycleManager lifecycle = new PostgresSnapshotLifecycleManager(connectorConfig, connectionFactory, taskContext, snapshotterService);
            // connectionFactory (local, ~line 113) and taskContext (field, ~line 134) are both in scope here
            final SnapshotCoordination prepCoordination = this.snapshotCoordination;
            final ErrorHandler leaderErrorHandler = this.errorHandler;
            this.smartLifecycleManager = lifecycle;

            this.smartSnapshotPreparationThread = new Thread(() -> {
                try {
                    // for debugging
                    taskContext.configureLoggingContext("snapshot-prep", new PostgresPartition(connectorConfig.getConnectorName(), "", "0"));

                    SnapshotLifecycleManager.SnapshotSetup setup = lifecycle.prepareSnapshot(allTables, shouldStream);

                    Map<String, Object> shared = new HashMap<>();
                    shared.put(SmartSnapshotConnectorCoordinator.SLOT_LSN_KEY, setup.consistentPosition());
                    shared.put(SmartSnapshotConnectorCoordinator.SNAPSHOT_NAME_KEY, setup.snapshotName());
                    shared.put(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY, false);
                    shared.put(SmartSnapshotConnectorCoordinator.EPOCH_KEY, leaderEpoch);
                    prepCoordination.write(Collect.hashMapOf("server", connectorConfig.getLogicalName()), shared);

                    LOGGER.info("Smart snapshot: [task-0] prepared snapshot='{}', lsn={}, epoch={}",
                            setup.snapshotName(), setup.consistentPosition(), leaderEpoch);

                    // hold connections open + keep them alive until the task stops
                    while (!Thread.currentThread().isInterrupted()) {
                        Thread.sleep(30_000);
                        lifecycle.keepAlive();
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                catch (Exception e) {
                    LOGGER.error("Smart snapshot [task-0]: snapshot preparation failed", e);
                    lifecycle.releaseSnapshot();
                    // Fail the task with the real error. We do NOT write restart_needed here: prep failed,
                    // so the snapshot was never published and there is nothing to throw away. When task-0
                    // restarts it sees its own marker and writes restart_needed then, which bumps the epoch.
                    leaderErrorHandler.setProducerThrowable(
                            new DebeziumException("Smart snapshot: [task-0] leader preparation failed", e));
                }
            }, "smart-snapshot-leader-prep");
            this.smartSnapshotPreparationThread.setDaemon(true);
            this.smartSnapshotPreparationThread.start();
        }

        try {
            // end the setup txn (guardrail query, etc.) so the snapshot's SET is the first
            jdbcConnection.commit();
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
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
                epoch, snapshotCoordination, connectorConfig.getTaskId(),
                connectorConfig.getLogicalName());

        coordinator.start(taskContext, this.queue, metadataProvider);
        return coordinator;
    }

    private PostgresOffsetContext fetchOffsetFromCoordinationTopic(
                                                                   Configuration config, PostgresPartition partition, PostgresConnectorConfig connectorConfig,
                                                                   Clock clock) {
        if (connectorConfig.isSmartSnapshotEnabled() && !isSmartSnapshotTask) {
            // Post-downscale streaming task: read LSN from coordination topic
            String coordinationTopic = connectorConfig.getLogicalName() + ".snapshot-coordination";
            String clientIdSuffix = connectorConfig.getLogicalName() + "-coordination-streaming";
            Map<String, Object> clientConfig = KafkaLogSnapshotCoordination.clientConfigFromOverrides(
                    config, connectorConfig.getSmartSnapshotCoordinationBootstrapServers());
            KafkaLogSnapshotCoordination kafkaLog = new KafkaLogSnapshotCoordination(clientConfig, coordinationTopic, clientIdSuffix);
            kafkaLog.start();
            Map<String, Object> coordData = kafkaLog.read(partition.getSourcePartition());
            kafkaLog.stop();

            if (coordData != null
                    && Boolean.TRUE.equals(coordData.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY))
                    && coordData.get(SmartSnapshotConnectorCoordinator.SLOT_LSN_KEY) != null) {
                String lsnStr = String.valueOf(coordData.get(SmartSnapshotConnectorCoordinator.SLOT_LSN_KEY));
                Lsn lsn = Lsn.valueOf(lsnStr);
                LOGGER.info("Smart snapshot [task-{}]: post-downscale streaming task, using LSN={} from coordination topic", connectorConfig.getTaskId(), lsn);
                // Create synthetic offset — snapshot completed, start streaming from this LSN
                PostgresOffsetContext syntheticOffset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, clock);
                syntheticOffset.updateWalPosition(
                        lsn, null,
                        clock.currentTimeAsInstant(),
                        null, null, null, null);
                syntheticOffset.postSnapshotCompletion();
                return syntheticOffset;
            }
        }

        return null;
    }

    private static List<TableId> parseTableList(String manifest) {
        if (manifest == null || manifest.isEmpty()) {
            return List.of();
        }
        return Arrays.stream(manifest.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(TableId::parse) // parse(str) => useCatalogBeforeSchema=true (matches PG db.schema.table)
                .collect(Collectors.toList());
    }

    private void doStopSmartSnapshot() {
        if (smartSnapshotPreparationThread != null) {
            smartSnapshotPreparationThread.interrupt();
            try {
                smartSnapshotPreparationThread.join(5000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            smartSnapshotPreparationThread = null;
        }
        if (smartLifecycleManager != null) {
            smartLifecycleManager.releaseSnapshot();
            smartLifecycleManager = null;
        }

        if (snapshotCoordination != null) {
            snapshotCoordination.stop();
        }
    }
}
