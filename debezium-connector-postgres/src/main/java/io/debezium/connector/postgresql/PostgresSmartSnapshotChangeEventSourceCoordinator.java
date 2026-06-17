/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

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
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Collect;
import io.debezium.util.LoggingContext;

public class PostgresSmartSnapshotChangeEventSourceCoordinator
        extends PostgresChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            PostgresSmartSnapshotChangeEventSourceCoordinator.class);
    private static final int retryCount = 30;

    private final int epoch;
    private final SnapshotCoordination snapshotCoordination;
    private final String taskId;
    private final String serverName;

    public PostgresSmartSnapshotChangeEventSourceCoordinator(
                                                             Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                                             ErrorHandler errorHandler,
                                                             Class<? extends SourceConnector> connectorType,
                                                             CommonConnectorConfig connectorConfig,
                                                             PostgresChangeEventSourceFactory changeEventSourceFactory,
                                                             ChangeEventSourceMetricsFactory<PostgresPartition> changeEventSourceMetricsFactory,
                                                             EventDispatcher<PostgresPartition, ?> eventDispatcher,
                                                             DatabaseSchema<?> schema,
                                                             SnapshotterService snapshotterService,
                                                             SlotState slotInfo,
                                                             SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor,
                                                             NotificationService<PostgresPartition, PostgresOffsetContext> notificationService,
                                                             int epoch,
                                                             SnapshotCoordination snapshotCoordination,
                                                             String taskId,
                                                             String serverName) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig,
                changeEventSourceFactory, changeEventSourceMetricsFactory,
                eventDispatcher, schema, snapshotterService, slotInfo,
                signalProcessor, notificationService);
        this.epoch = epoch;
        this.snapshotCoordination = snapshotCoordination;
        this.taskId = taskId;
        this.serverName = serverName;
    }

    @Override
    protected void executeChangeEventSources(
                                             CdcSourceTaskContext taskContext,
                                             SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> snapshotSource,
                                             Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                             AtomicReference<LoggingContext.PreviousContext> previousLogContext,
                                             ChangeEventSourceContext context)
            throws InterruptedException {

        PostgresPartition partition = previousOffsets.getTheOnlyPartition();
        previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));

        snapshotCoordination.start();

        PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        // Epoch mismatch with previous offset → stale from previous round, clear it
        if (previousOffset != null) {
            Integer offsetEpoch = previousOffset.getEpoch();
            if (offsetEpoch != null && !offsetEpoch.equals(epoch)) {
                LOGGER.info("Smart snapshot [task-{}]: epoch mismatch (offset={}, config={}), clearing offset",
                        taskId, offsetEpoch, epoch);
                previousOffsets.resetOffset(partition);
                previousOffset = null;
            }
        }

        if (previousOffset != null) {
            previousOffset.setEpoch(epoch);
        }

        // Read snapshot_name + LSN from coordination topic
        Map<String, String> sharedPartition = Collect.hashMapOf("server", serverName);
        String snapshotName = null;
        String slotLsnStr = null;
        for (int attempt = 0; attempt < retryCount; attempt++) {
            Map<String, Object> coordData = snapshotCoordination.read(sharedPartition);
            if (coordData != null
                    && coordData.get(SmartSnapshotConnectorCoordinator.SNAPSHOT_NAME_KEY) != null) {
                Integer coordEpoch = SmartSnapshotConnectorCoordinator.readEpoch(coordData);
                if (coordEpoch != null && coordEpoch == epoch) {
                    snapshotName = (String) coordData.get(
                            SmartSnapshotConnectorCoordinator.SNAPSHOT_NAME_KEY);
                    slotLsnStr = String.valueOf(coordData.get(
                            SmartSnapshotConnectorCoordinator.SLOT_LSN_KEY));
                    break;
                }
            }
            LOGGER.info("Smart snapshot [task-{}]: waiting for snapshot preparation (attempt {}/30)", taskId, attempt + 1);
            Thread.sleep(1000);
        }
        if (snapshotName == null) {
            throw new DebeziumException(String.format("Smart snapshot [task-%s]: Timed out waiting for snapshot preparation", taskId));
        }

        LOGGER.info("Smart snapshot [task-{}]: got snapshot='{}', LSN={}, executing snapshot-only, epoch={}",
                taskId, snapshotName, slotLsnStr, epoch);

        // Set snapshot name, LSN, and coordination on the source
        PostgresSmartSnapshotChangeEventSource smartSource = (PostgresSmartSnapshotChangeEventSource) snapshotSource;
        smartSource.setSmartSnapshotName(snapshotName);
        smartSource.setSmartSnapshotLsn(Lsn.valueOf(slotLsnStr));
        smartSource.setSnapshotCoordination(snapshotCoordination, epoch);

        // Run snapshot
        SnapshotResult<PostgresOffsetContext> snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);
        LOGGER.info("Smart snapshot [task-{}]: snapshot completed with status={}", taskId, snapshotResult.getStatus());

        /**
         catch block after doSnapshot required in Mysql implementation (remove later)
        catch (Exception e) {
            // Stale snapshot (e.g. SET TRANSACTION SNAPSHOT failed)

            if (requiresFullRestartOnTaskFailure()) {
                // MySQL: task can't rejoin, signal restart to Connector via coordination topic
                LOGGER.warn("Smart snapshot: task {} failed, signaling restart_needed", taskId, e);
                try {
                    Map<String, String> taskPartition = partition.getSourcePartition();
                    Map<String, Object> restartSignal = new HashMap<>();
                    restartSignal.put("restart_needed", true);
                    restartSignal.put(SmartSnapshotConnectorCoordinator.EPOCH_KEY, epoch);
                    snapshotCoordination.write(taskPartition, restartSignal);
                }
                catch (Exception writeEx) {
                    LOGGER.error("Failed to write restart_needed signal", writeEx);
                }
            }
            throw e; // Re-throw — task fails, Connect restarts it
        } **/

        // transaction_started signal already sent from lockTablesForSchemaSnapshot()
        // No streaming. Task idles until monitor detects completion.
    }
}
