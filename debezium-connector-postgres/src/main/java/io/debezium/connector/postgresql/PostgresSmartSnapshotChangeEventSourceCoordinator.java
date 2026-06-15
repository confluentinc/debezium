/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Collect;
import io.debezium.util.LoggingContext;

/**
 * Coordinator for multi-task smart snapshot mode. Extends {@link PostgresChangeEventSourceCoordinator}
 * to run snapshot only (no streaming) and coordinate via the offset topic.
 *
 * <p>Overrides {@link #executeChangeEventSources} to:
 * <ul>
 *   <li>Leader: write coordination data (LSN, snapshot_name, epoch) to {@code {"server":"<prefix>"}}
 *       before snapshot, and update {@code snapshot_completed=true} after.</li>
 *   <li>All tasks: run {@code doSnapshot()} then idle, no streaming.</li>
 *   <li>On stale snapshot error ({@code SET TRANSACTION SNAPSHOT} fails):
 *       write {@code restart_required=true} to trigger full reconfiguration via the Connector's monitor thread.</li>
 * </ul>
 *
 * <p>Used only when {@code smart.snapshot=true} and {@code tasks.max > 1}. Single-task mode
 * uses the parent {@link PostgresChangeEventSourceCoordinator} unchanged.
 */
public class PostgresSmartSnapshotChangeEventSourceCoordinator extends PostgresChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSmartSnapshotChangeEventSourceCoordinator.class);

    private final String taskId;
    private final boolean isLeader;
    private final SnapshotCoordination snapshotCoordination;
    private final Lsn slotLsn;
    private final String snapshotName;
    private final int epoch;
    private final boolean restartRequired;

    public PostgresSmartSnapshotChangeEventSourceCoordinator(
                                                             Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                                             ErrorHandler errorHandler, Class<? extends SourceConnector> connectorType,
                                                             CommonConnectorConfig connectorConfig,
                                                             PostgresChangeEventSourceFactory changeEventSourceFactory,
                                                             ChangeEventSourceMetricsFactory<PostgresPartition> changeEventSourceMetricsFactory,
                                                             EventDispatcher<PostgresPartition, ?> eventDispatcher, DatabaseSchema<?> schema,
                                                             SnapshotterService snapshotterService,
                                                             SlotState slotInfo,
                                                             SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor,
                                                             NotificationService<PostgresPartition, PostgresOffsetContext> notificationService,
                                                             String taskId,
                                                             boolean isLeader,
                                                             SnapshotCoordination snapshotCoordination,
                                                             Lsn slotLsn,
                                                             String snapshotName,
                                                             int epoch,
                                                             boolean restartRequired) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory, changeEventSourceMetricsFactory, eventDispatcher, schema,
                snapshotterService, slotInfo, signalProcessor, notificationService);
        this.taskId = taskId;
        this.isLeader = isLeader;
        this.snapshotCoordination = snapshotCoordination;
        this.slotLsn = slotLsn;
        this.snapshotName = snapshotName;
        this.epoch = epoch;
        this.restartRequired = restartRequired;
    }

    /**
     * Executes snapshot-only mode for multi-task smart snapshot. The flow depends on task state:
     *
     * <p><b>If restartRequired:</b> writes {@code restart_required=true} to the shared key
     * via coordination, then returns immediately. The coordinator thread exits, but the task
     * stays alive — {@code poll()} continues running and flushes the record to the offset topic.
     * The Connector's monitor thread detects it and triggers full reconfiguration with a new
     epoch.
     *
     * <p><b>Normal flow:</b>
     * <ol>
     *   <li>Leader writes coordination data (LSN, snapshot_name, epoch) to {@code
    {"server":"<prefix>"}}</li>
     *   <li>Sets epoch on the offset context so per-task offsets include it</li>
     *   <li>Runs {@code doSnapshot()} — all tasks snapshot their assigned tables using
     *     {@code SET TRANSACTION SNAPSHOT} from the synthetic SlotCreationResult</li>
     *   <li>Returns — task idles (no streaming). Monitor detects per-task completion
     *     and triggers downscale.</li>
     * </ol>
     *
     * <p><b>On stale snapshot error:</b> if {@code SET TRANSACTION SNAPSHOT} fails (snapshot
     * no longer valid — e.g., leader crashed and replication connection closed), writes
     * {@code restart_required=true} to the shared key and rethrows. The task fails, but the
     * record is flushed before shutdown. Monitor detects it and triggers reconfiguration.
     */
    @Override
    protected void executeChangeEventSources(
                                             CdcSourceTaskContext taskContext,
                                             SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> snapshotSource,
                                             Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                             AtomicReference<LoggingContext.PreviousContext> previousLogContext,
                                             ChangeEventSource.ChangeEventSourceContext context)
            throws InterruptedException {
        if (restartRequired) {
            LOGGER.warn("Smart snapshot: task-{} detected that a restart is required, writing restart_required=true and idling", taskId);
            // Idle until Connect stops this task (monitor will trigger reconfiguration)
            try {
                Map<String, Object> existingData = snapshotCoordination.read(Collect.hashMapOf("server", connectorConfig.getLogicalName()));
                if (existingData != null) {
                    Map<String, Object> updated = new HashMap<>(existingData);
                    updated.put(PostgresConnector.RESTART_KEY, true);
                    snapshotCoordination.write(Collect.hashMapOf("server", connectorConfig.getLogicalName()), updated);
                }
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot: task-{} failed to write restart_required", taskId, e);
            }
            return;
            // the flow after return
            // Coordinator thread: exits
            // Poll thread: still running → drains queue → restart_required record flushed
            // Monitor: detects restart_required → requestTaskReconfiguration()
            // Connect: stops all tasks → taskConfigs() → new epoch → clean restart
        }

        // smart snapshot mode: snapshot only, then idle
        final PostgresPartition partition = previousOffsets.getTheOnlyPartition();
        final PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        LOGGER.info("Smart snapshot: task-{} entering snapshot phase", taskId);

        // Leader: write coordination data before snapshot
        if (isLeader) {
            try {
                Map<String, Object> coordinationData = new HashMap<>();
                coordinationData.put(SourceInfo.LSN_KEY, slotLsn.asLong());
                coordinationData.put(PostgresConnector.SNAPSHOT_NAME_KEY, snapshotName);
                coordinationData.put(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY, false);
                coordinationData.put(PostgresConnector.EPOCH_KEY, epoch);
                snapshotCoordination.write(Collect.hashMapOf("server", connectorConfig.getLogicalName()), coordinationData);
                LOGGER.info("Smart snapshot: Leader wrote coordination data with LSN={}, snapshot_name={}, epoch={}", slotLsn, snapshotName, epoch);
            }
            catch (Exception e) {
                throw new DebeziumException("Smart snapshot: Leader failed to write coordination data", e);
            }
        }

        SnapshotResult<PostgresOffsetContext> snapshotResult;

        try {
            previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));
            snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);
        }
        catch (Exception e) {
            // Check if this is a stale snapshot error (SET TRANSACTION SNAPSHOT failed)
            if (isStaleSnapshotError(e)) {
                LOGGER.warn("Smart snapshot: task-{} snapshot failed due to stale snapshot, writing restart_required=true to trigger reconfiguration.", taskId, e);

                try {
                    Map<String, Object> existingData = snapshotCoordination.read(Collect.hashMapOf("server", connectorConfig.getLogicalName()));
                    if (existingData != null) {
                        Map<String, Object> updated = new HashMap<>(existingData);
                        updated.put(PostgresConnector.RESTART_KEY, true);
                        snapshotCoordination.write(Collect.hashMapOf("server", connectorConfig.getLogicalName()), updated);
                        LOGGER.info("Smart snapshot: task-{} wrote restart_required=true to coordination data", taskId);
                    }
                }
                catch (Exception coordinatorException) {
                    LOGGER.warn("Smart snapshot: task-{} failed to write restart_required to coordination data", taskId, coordinatorException);
                }
            }
            throw e;
        }

        LOGGER.info("Smart snapshot: task-{} completed with result {}, entering idle mode", taskId, snapshotResult);
    }

    /**
     * Checks whether the exception was caused by a stale Postgres snapshot, specifically,
     * a {@code SET TRANSACTION SNAPSHOT '<name>'} that failed because the exporting
     * transaction is no longer alive (e.g., leader crashed, replication connection closed).
     * Postgres reports this as an error containing "snapshot" and "does not exist".
     */
    private boolean isStaleSnapshotError(Exception e) {
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof SQLException) {
                String msg = cause.getMessage();
                if (msg != null && msg.contains("snapshot") && msg.contains("does not exist")) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }
}
