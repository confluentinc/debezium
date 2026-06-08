/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
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
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.GuardrailValidator;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.OffsetTopicSnapshotCoordination;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
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
    private volatile PostgresConnection snapshotHolderConnection = null;

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

        isSmartSnapshotTask = connectorConfig.isSmartSnapshotEnabled() && connectorConfig.getTaskId() != null;
        if (isSmartSnapshotTask) {
            return startSmartSnapshotTask(
                    config,
                    connectorConfig,
                    snapshotterService,
                    previousOffsets,
                    topicNamingStrategy,
                    previousContext,
                    threadNameContext,
                    connectionFactory);
        }

        try {
            SlotState slotInfo = getSlotState(connectorConfig);

            SlotCreationResult slotCreatedInfo = tryToCreateSlot(snapshotter, connectorConfig, slotInfo);

            try {
                jdbcConnection.commit();
            }
            catch (SQLException e) {
                throw new DebeziumException(e);
            }

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

    /**
     * Starts a smart snapshot task in multi-task mode. Called when smart.snapshot.enabled=true
     * and task.id is present in config.
     *
     * <p>Task-0 is the leader, all others are followers. The leader creates the replication slot and
     * exports a shared PostgreSQL snapshot that all tasks join via SET TRANSACTION SNAPSHOT. After
     * snapshot completes, all tasks idle — no streaming. The Connector's monitor thread detects
     * completion and triggers downscale to a single streaming task.
     *
     * <p>The method has five phases:
     *
     * <p><b>1. CONFIG + STALE OFFSET CHECK</b>
     * <br>Reads epoch and coordinationState from config (set by Connector in taskConfigs).
     * Checks if this task already completed snapshot for the current epoch (single-task restart
     * where the task finished but was restarted without reconfiguration). If completed, keeps
     * the previous offset so the snapshot source skips and the task idles. Otherwise, clears
     * the previous offset to force a fresh snapshot.
     *
     * <p><b>2. INFRASTRUCTURE SETUP</b>
     * <br>Standard Debezium components: queue, error handler, signal processor, dispatcher,
     * notification service, and SnapshotCoordination (backed by the offset topic via heartbeat
     * records). Created before slot/snapshot logic since none depend on slot state.
     *
     * <p><b>3. LEADER: SLOT + SNAPSHOT CREATION</b>
     * <br>Three scenarios:
     * <ul>
     *   <li><b>NEW + slot just created:</b> snapshot_name comes from CREATE_REPLICATION_SLOT
     *     (by-product of slot creation). The replication connection's implicit transaction
     *     holds the snapshot alive.</li>
     *   <li><b>RESTART or slot pre-created (no single-task restart):</b> slot already exists,
     *     no snapshot from slot creation. Opens a dedicated snapshotHolderConnection, calls
     *     pg_export_snapshot() to create a new exportable snapshot. This connection's transaction
     *     must stay open for the entire snapshot phase (closed in doStop). LSN is read from
     *     the slot's flushed position.</li>
     *   <li><b>Single-task restart within same epoch:</b> coordination data already exists for
     *     this epoch — a previous leader instance wrote it. Creating a new snapshot would change
     *     the LSN, causing data loss for followers that already snapshotted at the old LSN.
     *     Sets restartRequired=true — the coordinator writes restart_required to the shared key
     *     and idles. The monitor detects it and triggers full reconfiguration with a new epoch.</li>
     * </ul>
     * In all cases, a SlotCreationResult is produced (real or synthetic) so the existing snapshot
     * code path (SET TRANSACTION SNAPSHOT in PostgresSnapshotChangeEventSource) works unchanged.
     * When restartRequired=true, SlotCreationResult is null (coordinator returns before using it).
     *
     * <p><b>4. FOLLOWER: POLL FOR SNAPSHOT</b>
     * <br>Skips replication connection and slot creation entirely. Polls {"server":"&lt;prefix&gt;"}
     * via SnapshotCoordination until snapshot_name appears with a matching epoch (ignores stale
     * data from previous epochs and stale restart_required flags). Creates a synthetic
     * SlotCreationResult with the leader's snapshot_name + LSN.
     *
     * <p><b>5. COORDINATOR CREATION</b>
     * <br>Creates PostgresSmartSnapshotChangeEventSourceCoordinator which overrides
     * executeChangeEventSources to:
     * <ul>
     *   <li>If restartRequired: write restart_required to shared key, return (idle). poll()
     flushes
     *     the record, monitor detects it, triggers reconfiguration.</li>
     *   <li>If leader: write coordination data (LSN, snapshot_name, epoch) before snapshot.</li>
     *   <li>Run doSnapshot() with epoch set on the offset context (so per-task offsets include
     epoch).</li>
     *   <li>Idle after snapshot — no streaming.</li>
     *   <li>On stale snapshot error (SET TRANSACTION SNAPSHOT fails): write restart_required to
     *     trigger full reconfiguration via the monitor thread.</li>
     * </ul>
     */
    private ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> startSmartSnapshotTask(
                                                                                                          Configuration config,
                                                                                                          PostgresConnectorConfig connectorConfig,
                                                                                                          SnapshotterService snapshotterService,
                                                                                                          Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                                                                                          TopicNamingStrategy<TableId> topicNamingStrategy,
                                                                                                          LoggingContext.PreviousContext previousContext,
                                                                                                          ThreadNameContext threadNameContext,
                                                                                                          MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory) {
        final Clock clock = Clock.system();
        String taskId = connectorConfig.getTaskId();
        boolean isLeader = "0".equals(taskId);

        // ── Phase 1: Read config ──────────────────────────────────────────────
        // taskId, epoch, and coordinationState are set by the Connector in taskConfigs().
        // taskId=0 is the leader (creates slot, writes coordination data).
        // epoch is the coordination round number — incremented on full restart.
        // coordinationState is NEW (first start) or RESTART (after failure/reconfiguration).

        // current epoch round set by the connector
        int epoch = Integer.parseInt(config.getString(PostgresConnector.EPOCH_KEY, "1"));
        // NEW or RESTART set by the connector
        String coordinationState = config.getString(PostgresConnector.COORDINATION_STATE_KEY, PostgresConnector.COORDINATION_STATE_NEW);

        LOGGER.info("Smart snapshot: Starting taskId={}, isLeader={}, epoch={}, coordinationState={}", taskId, isLeader, epoch, coordinationState);

        // ── Phase 2: Check if this task already completed for this epoch ──────
        // On single-task restart (Connect restarts just this task, same config),
        // the per-task offset may have snapshot_completed=true from this epoch.
        // If so, the task should idle — not re-snapshot.
        // For all other cases (RESTART with new epoch, first start, incomplete),
        // clear the previous offset so the snapshot source treats it as a fresh start.
        PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();
        boolean alreadyCompletedThisEpoch = previousOffset != null
                && previousOffset.isSnapshotCompleted()
                && previousOffset.getEpoch() != null
                && previousOffset.getEpoch() == epoch;

        if (alreadyCompletedThisEpoch) {
            LOGGER.info("Smart snapshot: task-{}, already completed snapshot for epoch {}, will idle", taskId, epoch);
        }
        else {
            LOGGER.info("Smart snapshot: task-{}, clearing previous offset, will snapshot fresh", taskId);
            previousOffsets = Offsets.of(Collections.singletonMap(previousOffsets.getTheOnlyPartition(), null));
        }

        // ── Phase 3: Standard Debezium infrastructure ─────────────────────────
        // Queue, error handler, signal processor, dispatcher, notification service.
        // Also creates SnapshotCoordination (backed by offset topic via heartbeat records).
        // These don't depend on slot state or snapshot name — safe to create early.
        queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        // it is assumed that heartbeat is enabled, the validation is present in the connector
        String heartbeatTopicName = connectorConfig.getHeartbeatTopicsPrefix() + "." + connectorConfig.getLogicalName();
        SnapshotCoordination snapshotCoordination = new OffsetTopicSnapshotCoordination(
                queue, context.offsetStorageReader(), connectorConfig.getLogicalName(),
                heartbeatTopicName);

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
                        connectorConfig.schemaNameAdjuster(),
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
                connectorConfig.schemaNameAdjuster(),
                signalProcessor,
                connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

        NotificationService<PostgresPartition, PostgresOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        // ── Phase 4: Slot state + leader/follower branching ───────────────────
        // Leader and follower take different paths to obtain the snapshot name + LSN:
        //
        // LEADER (taskId=0):
        // Path A: coordinationState=NEW, slot just created
        // → snapshot_name + LSN come from CREATE_REPLICATION_SLOT (SlotCreationResult)
        // → replication connection's implicit transaction holds the snapshot alive
        //
        // Path B: coordinationState=RESTART, or slot pre-created/already exists
        // → first checks if coordination data exists for this epoch (single-task restart)
        // if yes → sets restartRequired=true (coordinator will write restart_required and idle)
        // if no → opens dedicated snapshotHolderConnection, calls pg_export_snapshot()
        // to get a new snapshot_name. LSN from slot's flushed position.
        //
        // In both paths, a SlotCreationResult (real or synthetic) is produced so the
        // existing PostgresSnapshotChangeEventSource code path (SET TRANSACTION SNAPSHOT)
        // works unchanged.
        //
        // FOLLOWER (taskId != 0):
        // → skips slot creation and replication connection entirely
        // → polls {"server":"<prefix>"} via SnapshotCoordination until snapshot_name
        // appears with matching epoch
        // → creates synthetic SlotCreationResult with the leader's snapshot_name + LSN
        try {
            SlotCreationResult slotCreatedInfo;
            String snapshotName = null;
            Lsn originalSlotLsn = null;

            SlotState slotInfo = getSlotState(connectorConfig);

            try {
                jdbcConnection.commit();
            }
            catch (SQLException e) {
                throw new DebeziumException(e);
            }

            boolean restartRequired = false;
            if (isLeader) {
                // Leader: create replication connection and possibly the slot
                // Determine snapshot name and LSN based on coordination state
                slotCreatedInfo = tryToCreateSlot(snapshotterService.getSnapshotter(), connectorConfig, slotInfo);

                if (PostgresConnector.COORDINATION_STATE_NEW.equals(coordinationState) && slotCreatedInfo != null) {
                    // First start: slot just created, snapshot from slot creation
                    snapshotName = slotCreatedInfo.snapshotName();
                    originalSlotLsn = slotCreatedInfo.startLsn();
                    LOGGER.info("Smart snapshot leader: using slot snapshot '{}', LSN={}",
                            snapshotName, originalSlotLsn);
                }
                else {
                    // Path B: slot already exists (restart or pre-created by DBA).
                    // No snapshot_name from CREATE_REPLICATION_SLOT — need pg_export_snapshot().
                    //
                    // But first: detect single-task restart within the same epoch.
                    // If coordination data already exists for this epoch, a previous leader
                    // instance already wrote it. Creating a new snapshot would change the LSN,
                    // causing data loss for followers that snapshotted at the old LSN.
                    // Instead, set restartRequired=true — the coordinator will write
                    // restart_required to the shared key and idle. The monitor detects it
                    // and triggers full reconfiguration with a new epoch.
                    try {
                        Map<String, Object> existingData = snapshotCoordination.readSharedData();
                        if (existingData != null) {
                            Integer existingEpoch = PostgresConnector.readEpoch(existingData);
                            if (existingEpoch != null && existingEpoch == epoch) {
                                LOGGER.warn(
                                        "Smart snapshot: Leader restart detected, that coordination data exists for epoch {}. Writing restart_required to trigger full reconfiguration.",
                                        epoch);
                                restartRequired = true;
                            }
                        }
                    }
                    catch (Exception e) {
                        LOGGER.warn("Smart snapshot: Leader failed to check existing coordination data, proceeding with new snapshot", e);
                    }

                    if (!restartRequired) {
                        // No existing data for this epoch safe to create new snapshot
                        try {
                            // create a new jdbc connection for creating snapshot
                            snapshotHolderConnection = new PostgresConnection(connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL, threadNameContext);
                            snapshotHolderConnection.connection().setAutoCommit(false);
                            snapshotHolderConnection.executeWithoutCommitting("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                            snapshotName = snapshotHolderConnection.queryAndMap(
                                    "SELECT pg_export_snapshot()",
                                    snapshotHolderConnection.singleResultMapper(rs -> rs.getString(1),
                                            "Could not export snapshot"));
                            LOGGER.info("Smart snapshot: Leader exported snapshot '{}' via pg_export_snapshot()", snapshotName);
                        }
                        catch (SQLException e) {
                            throw new DebeziumException("Smart snapshot: Leader failed to export snapshot via pg_export_snapshot()", e);
                        }

                        if (slotInfo == null) {
                            throw new DebeziumException("Smart snapshot: Leader detected that slot does not exist and was not created");
                        }
                        originalSlotLsn = slotInfo.slotLastFlushedLsn();

                        // Create synthetic SlotCreationResult for existing code path
                        slotCreatedInfo = new SlotCreationResult(
                                connectorConfig.slotName(),
                                originalSlotLsn.asString(),
                                snapshotName,
                                connectorConfig.plugin().getPostgresPluginName());
                    }
                }
            }
            else {
                // Follower: skip replication connection and slot creation entirely
                LOGGER.info("Smart snapshot: follower skipping slot creation, waiting for leader coordination");

                // Follower: poll coordination for snapshot_name
                try {
                    snapshotName = pollForSnapshotName(snapshotCoordination, epoch);
                    Map<String, Object> sharedData = snapshotCoordination.readSharedData();
                    Long lsnValue = (Long) sharedData.get(SourceInfo.LSN_KEY);
                    originalSlotLsn = Lsn.valueOf(lsnValue);
                }
                catch (Exception e) {
                    throw new DebeziumException("Smart snapshot: follower failed to read coordination data", e);
                }

                slotCreatedInfo = new SlotCreationResult(
                        connectorConfig.slotName(),
                        originalSlotLsn.asString(),
                        snapshotName,
                        connectorConfig.plugin().getPostgresPluginName());
                LOGGER.info("Smart snapshot: follower joined leader snapshot '{}', LSN={}",
                        snapshotName, originalSlotLsn);
            }

            // ── Phase 5: Create coordinator and start ─────────────────────────
            // PostgresSmartSnapshotChangeEventSourceCoordinator handles:
            // - If restartRequired: writes restart_required to shared key, returns (idles)
            // - If leader: writes coordination data (LSN, snapshot_name, epoch) before
            // snapshot, runs doSnapshot(), then idles (no streaming)
            // - If follower: runs doSnapshot() (using SET TRANSACTION SNAPSHOT from
            // synthetic SlotCreationResult), then idles
            // - On stale snapshot error: writes restart_required to trigger reconfiguration
            ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> coordinator = new PostgresSmartSnapshotChangeEventSourceCoordinator(
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
                    taskId,
                    isLeader,
                    snapshotCoordination,
                    originalSlotLsn,
                    snapshotName,
                    epoch,
                    restartRequired);

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

    /**
     * Polls the shared coordination key {@code {"server":"<prefix>"}} via {@link SnapshotCoordination}
     * until a {@code snapshot_name} appears with a matching epoch. Used by follower tasks to wait
     * for the leader to create the slot and write coordination data.
     *
     * <p>If {@code restart_required=true} is detected in the shared key, the follower keeps
     polling
     * (the monitor thread will trigger reconfiguration and Connect will stop this task).
     *
     * @param coordination the coordination abstraction for reading the shared key
     * @param expectedEpoch the epoch from this task's config — only data with matching epoch is
    accepted
     * @return the snapshot_name from the leader's coordination data
     * @throws DebeziumException if polling times out (5 minutes) or is interrupted
     */
    private String pollForSnapshotName(SnapshotCoordination coordination, int expectedEpoch) {
        LOGGER.info("Smart snapshot: Follower polling for snapshot_name with epoch={}", expectedEpoch);
        final Metronome metronome = Metronome.parker(Duration.ofSeconds(5), Clock.SYSTEM);
        final long timeoutMs = Duration.ofMinutes(5).toMillis();
        final long startTime = System.currentTimeMillis();

        while (true) {
            try {
                Map<String, Object> sharedData = coordination.readSharedData();
                if (sharedData != null) {
                    // If restart_required, don't try to join — wait for reconfiguration
                    if (Boolean.TRUE.equals(sharedData.get(PostgresConnector.RESTART_KEY))) {
                        LOGGER.info("Smart snapshot: Follower detected restart_required, waiting for reconfiguration");
                        // Keep polling — monitor will trigger reconfiguration, Connect will stop this task
                    }
                    else {
                        Integer dataEpoch = sharedData.get(PostgresConnector.EPOCH_KEY) != null
                                ? ((Number) sharedData.get(PostgresConnector.EPOCH_KEY)).intValue()
                                : null;
                        String name = (String) sharedData.get(PostgresConnector.SNAPSHOT_NAME_KEY);

                        if (name != null && dataEpoch != null && dataEpoch == expectedEpoch) {
                            LOGGER.info("Smart snapshot: Follower received coordination data — snapshot_name='{}', epoch={}, LSN={}",
                                    name, dataEpoch, sharedData.get(SourceInfo.LSN_KEY));
                            return name;
                        }
                    }
                }

                if (System.currentTimeMillis() - startTime > timeoutMs) {
                    throw new DebeziumException("Smart snapshot: Follower timed out waiting for leader coordination data after " + timeoutMs + "ms");
                }

                metronome.pause();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DebeziumException("Smart snapshot: Follower interrupted while waiting for coordination data", e);
            }
            catch (Exception e) {
                if (e instanceof DebeziumException) {
                    throw (DebeziumException) e;
                }
                throw new DebeziumException("Smart snapshot: Follower error reading coordination data", e);
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
