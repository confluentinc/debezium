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
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresConnection.PostgresValueConverterBuilder;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.GuardrailValidator;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.source.snapshot.OffsetTopicSnapshotCoordination;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
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

    private volatile PostgresTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile PostgresConnection jdbcConnection;
    private volatile PostgresConnection beanRegistryJdbcConnection;
    private volatile ReplicationConnection replicationConnection = null;

    private volatile ErrorHandler errorHandler;
    private volatile PostgresSchema schema;

    private Partition.Provider<PostgresPartition> partitionProvider = null;
    private OffsetContext.Loader<PostgresOffsetContext> offsetContextLoader = null;

    private final ReentrantLock commitLock = new ReentrantLock();
    private boolean isSmartSnapshotTask;
    private boolean isLeader;
    private int epoch;
    private String coordinationState;
    private volatile PostgresConnection snapshotHolderConnection = null;
    private SnapshotCoordination snapshotCoordination = null;

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
        this.taskContext = new PostgresTaskContext(connectorConfig, schema, topicNamingStrategy);
        this.partitionProvider = new PostgresPartition.Provider(connectorConfig, config);
        this.offsetContextLoader = new PostgresOffsetContext.Loader(connectorConfig);
        final Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets = getPreviousOffsets(
                this.partitionProvider, this.offsetContextLoader);
        final Clock clock = Clock.system();
        final PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

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

        // Smart snapshot role detection
        String taskId = connectorConfig.getTaskId();
        isSmartSnapshotTask = connectorConfig.isSmartSnapshotEnabled() && taskId != null;
        isLeader = !isSmartSnapshotTask || "0".equals(taskId);

        if (isSmartSnapshotTask) {
            epoch = Integer.parseInt(config.getString(PostgresConnector.EPOCH_KEY, "1"));
            coordinationState = config.getString(PostgresConnector.COORDINATION_STATE_KEY, PostgresConnector.COORDINATION_STATE_NEW);
            LOGGER.info("Smart snapshot task: taskId={}, isLeader={}, epoch={}, coordinationState={}", taskId, isLeader, epoch, coordinationState);

            if (connectorConfig.getHeartbeatInterval().isZero()) {
                throw new DebeziumException("Heartbeat must be enabled (heartbeat.interval.ms > 0) when smart.snapshot=true");
            }
        }

        try {
            SlotState slotInfo = getSlotState(connectorConfig);
            SlotCreationResult slotCreatedInfo = null;
            String snapshotName = null;
            Lsn originalSlotLsn = null;

            if (isLeader || !isSmartSnapshotTask) {
                // Leader or single-task: create replication connection and possibly the slot
                slotCreatedInfo = tryToCreateSlot(snapshotter, connectorConfig, slotInfo);
            } else {
                // Follower: skip replication connection and slot creation entirely
                LOGGER.info("Smart snapshot follower: skipping slot creation, waiting for leader coordination");
            }

            if (isSmartSnapshotTask && isLeader) {
                // Determine snapshot name and LSN based on coordination state
                if ("RESTART".equals(coordinationState) || (slotCreatedInfo == null && slotInfo != null)) {
                    try {
                        // Epoch restart or slot pre-created: use pg_export_snapshot()
                        snapshotHolderConnection = new
                                PostgresConnection(connectorConfig.getJdbcConfig(),
                                PostgresConnection.CONNECTION_GENERAL, threadNameContext);
                        snapshotHolderConnection.connection().setAutoCommit(false);
                        snapshotName = snapshotHolderConnection.queryAndMap(
                                "SELECT pg_export_snapshot()",
                                snapshotHolderConnection.singleResultMapper(rs -> rs.getString(1),
                                        "Could not export snapshot"));
                        LOGGER.info("Smart snapshot leader: exported snapshot '{}' via pg_export_snapshot()", snapshotName);
                    } catch (SQLException e) {
                        throw new DebeziumException("Failed to export snapshot via pg_export_snapshot()", e);
                    }
                    if (slotInfo != null) {
                        originalSlotLsn = slotInfo.slotLastFlushedLsn();
                    } else {
                        try {
                            String lsnStr = jdbcConnection.queryAndMap(
                                    "SELECT pg_current_wal_lsn()::text",
                                    jdbcConnection.singleResultMapper(rs -> rs.getString(1), "Could not get current WAL LSN"));
                                            originalSlotLsn = Lsn.valueOf(lsnStr);
                        }
                        catch (SQLException e) {
                            throw new DebeziumException("Failed to get current WAL LSN", e);
                        }
                    }

                    // Create synthetic SlotCreationResult for existing code path
                    slotCreatedInfo = new SlotCreationResult(
                            connectorConfig.slotName(),
                            originalSlotLsn.toString(),
                            snapshotName,
                            connectorConfig.plugin().getPostgresPluginName());
                } else if (slotCreatedInfo != null) {
                    // First start: snapshot from slot creation
                    snapshotName = slotCreatedInfo.snapshotName();
                    originalSlotLsn = slotCreatedInfo.startLsn();
                    LOGGER.info("Smart snapshot leader: using slot snapshot '{}', LSN={}",
                            snapshotName, originalSlotLsn);

                    // Check for single-task restart within same epoch
                    // (coordinationState=NEW but coordination data already exists for this epoch)
                    if (snapshotCoordination != null) {
                        try {
                            Map<String, Object> existingData = snapshotCoordination.readSharedData();
                            if (existingData != null) {
                                Integer existingEpoch = existingData.get(PostgresConnector.EPOCH_KEY) != null
                                        ? ((Number) existingData.get(PostgresConnector.EPOCH_KEY)).intValue() : null;
                                if (existingEpoch != null && existingEpoch == epoch) {
                                    LOGGER.info("Smart snapshot: single-task restart detected within epoch {}, reusing existing coordination data", epoch);
                                    String existingSnapshotName = (String) existingData.get("snapshot_name");
                                    if (existingSnapshotName != null) {
                                        snapshotName = existingSnapshotName;
                                        Long lsnValue = (Long) existingData.get(SourceInfo.LSN_KEY);
                                        originalSlotLsn = Lsn.valueOf(lsnValue);
                                        slotCreatedInfo = new SlotCreationResult(
                                                connectorConfig.slotName(),
                                                originalSlotLsn.asString(),
                                                snapshotName,
                                                connectorConfig.plugin().getPostgresPluginName());
                                    }
                                }
                            }
                        } catch (Exception e) {
                            LOGGER.warn("Smart snapshot: failed to read existing coordination data, proceeding as new start", e);
                        }
                    }
                }
            }

            try {
                jdbcConnection.commit();
            } catch (SQLException e) {
                throw new DebeziumException(e);
            }

            queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                    .loggingContextSupplier(() ->
                            taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            errorHandler = new PostgresErrorHandler(connectorConfig, queue, errorHandler);

            final PostgresEventMetadataProvider metadataProvider = new
                    PostgresEventMetadataProvider();

            SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor = new
                    SignalProcessor<>(
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
                            () -> new PostgresConnection(connectorConfig.getJdbcConfig(),
                                    PostgresConnection.CONNECTION_GENERAL, threadNameContext),
                            exception -> {
                                String sqlErrorId = exception.getSQLState();
                                switch (sqlErrorId) {
                                    case "57P01":
                                        throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                                    case "57P03":
                                        throw new RetriableException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                                    default:
                                        break;
                                }
                            }),
                    schemaNameAdjuster,
                    signalProcessor,

                    connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

            NotificationService<PostgresPartition, PostgresOffsetContext> notificationService = new
                    NotificationService<>(getNotificationChannels(),
                    connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

            // Create coordination for smart snapshot tasks
            if (isSmartSnapshotTask) {
                String heartbeatTopicName = connectorConfig.getHeartbeatTopicsPrefix() + "." +
                        connectorConfig.getLogicalName();
                snapshotCoordination = new OffsetTopicSnapshotCoordination(
                        queue, context.offsetStorageReader(), connectorConfig.getLogicalName(),
                        heartbeatTopicName);

                // Follower: poll coordination for snapshot_name
                if (!isLeader) {
                    try {
                        snapshotName = pollForSnapshotName(snapshotCoordination, epoch);
                        Map<String, Object> sharedData = snapshotCoordination.readSharedData();
                        Long lsnValue = (Long) sharedData.get(SourceInfo.LSN_KEY);
                        originalSlotLsn = Lsn.valueOf(lsnValue);
                    } catch (Exception e) {
                        throw new DebeziumException("Smart snapshot follower: failed to read coordination data", e);
                    }

                    slotCreatedInfo = new SlotCreationResult(
                            connectorConfig.slotName(),
                            originalSlotLsn.asString(),
                            snapshotName,
                            connectorConfig.plugin().getPostgresPluginName());
                    LOGGER.info("Smart snapshot follower: joined leader snapshot '{}', LSN={}",
                            snapshotName, originalSlotLsn);
                }
            }

            ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> coordinator =
                    new PostgresChangeEventSourceCoordinator(
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
                            notificationService,
                            isSmartSnapshotTask,
                            isLeader,
                            snapshotCoordination,
                            originalSlotLsn,
                            snapshotName,
                            epoch);

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

    private String pollForSnapshotName(SnapshotCoordination coordination, int expectedEpoch) {
        LOGGER.info("Smart snapshot follower: polling for snapshot_name with epoch={}", expectedEpoch);
        final Metronome metronome = Metronome.parker(Duration.ofSeconds(5), Clock.SYSTEM);
        final long timeoutMs = Duration.ofMinutes(5).toMillis();
        final long startTime = System.currentTimeMillis();

        while (true) {
            try {
                Map<String, Object> sharedData = coordination.readSharedData();
                if (sharedData != null) {
                    // If restart_required, don't try to join — wait for reconfiguration
                    if (Boolean.TRUE.equals(sharedData.get("restart_required"))) {
                        LOGGER.info("Smart snapshot follower: restart_required detected, waiting for reconfiguration");
                        // Keep polling — monitor will trigger reconfiguration, Connect will stop this task
                    }
                    else {
                        Integer dataEpoch = sharedData.get("epoch") != null
                                ? ((Number) sharedData.get("epoch")).intValue() : null;
                        String name = (String) sharedData.get("snapshot_name");

                        if (name != null && dataEpoch != null && dataEpoch == expectedEpoch) {
                            return name;
                        }
                    }
                }

                if (System.currentTimeMillis() - startTime > timeoutMs) {
                    throw new DebeziumException("Smart snapshot follower: timed out waiting for leader coordination data after " + timeoutMs + "ms");
                }

                metronome.pause();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DebeziumException("Smart snapshot follower: interrupted while waiting for coordination data", e);
            }
            catch (Exception e) {
                if (e instanceof DebeziumException) {
                    throw (DebeziumException) e;
                }
                throw new DebeziumException("Smart snapshot follower: error reading coordination data", e);
            }
        }
    }

    public ReplicationConnection createReplicationConnection(PostgresTaskContext taskContext, int maxRetries, Duration retryDelay)
            throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        ReplicationConnection replicationConnection = null;
        while (retryCount <= maxRetries) {
            try {
                return taskContext.createReplicationConnection(jdbcConnection);
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
        try {
            if (snapshotHolderConnection != null) {
                snapshotHolderConnection.close();
            }
        }
        catch (Exception e) {
            LOGGER.trace("Error while closing snapshot holder connection", e);
        }
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
}
