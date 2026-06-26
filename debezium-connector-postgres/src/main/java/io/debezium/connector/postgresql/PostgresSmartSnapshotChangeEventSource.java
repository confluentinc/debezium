/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Collect;

public class PostgresSmartSnapshotChangeEventSource extends PostgresSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSmartSnapshotChangeEventSource.class);

    private final PostgresConnectorConfig connectorConfig;
    private final PostgresConnection jdbcConnection;
    private final String taskId;
    private String smartSnapshotName;
    private SnapshotCoordination snapshotCoordination;
    private int epoch;
    private Lsn smartSnapshotLsn;

    public PostgresSmartSnapshotChangeEventSource(
                                                  PostgresConnectorConfig connectorConfig,
                                                  SnapshotterService snapshotterService,
                                                  MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory,
                                                  PostgresSchema schema,
                                                  EventDispatcher<PostgresPartition, TableId> dispatcher,
                                                  Clock clock,
                                                  SnapshotProgressListener<PostgresPartition> snapshotProgressListener,
                                                  SlotCreationResult slotCreatedInfo,
                                                  SlotState startingSlotInfo,
                                                  NotificationService<PostgresPartition, PostgresOffsetContext> notificationService) {
        super(connectorConfig, snapshotterService, connectionFactory, schema,
                dispatcher, clock, snapshotProgressListener,
                slotCreatedInfo, startingSlotInfo, notificationService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
        this.taskId = connectorConfig.getTaskId();
    }

    public void setSmartSnapshotName(String snapshotName) {
        this.smartSnapshotName = snapshotName;
    }

    public void setSmartSnapshotLsn(Lsn lsn) {
        this.smartSnapshotLsn = lsn;
    }

    public void setSnapshotCoordination(SnapshotCoordination coordination, int epoch) {
        this.snapshotCoordination = coordination;
        this.epoch = epoch;
    }

    @Override
    protected void determineSnapshotOffset(
                                           RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx,
                                           PostgresOffsetContext previousOffset)
            throws Exception {
        // Create fresh offset with the Connector's slot LSN — not current WAL position
        PostgresOffsetContext offset = PostgresOffsetContext.initialContext(
                connectorConfig, jdbcConnection, getClock());
        offset.setEpoch(epoch);
        Long txId = jdbcConnection.currentTransactionId();
        offset.updateWalPosition(smartSnapshotLsn, null, getClock().currentTime(),
                txId, null, null, null);
        ctx.offset = offset;
        LOGGER.info("Smart snapshot: [task-{}] set offset LSN={}, epoch={}", taskId, smartSnapshotLsn, epoch);
    }

    @Override
    protected void setSnapshotTransactionIsolationLevel(boolean isOnDemand) throws SQLException {
        if (smartSnapshotName != null && !isOnDemand) {
            String snapSet = String.format("SET TRANSACTION SNAPSHOT '%s';", smartSnapshotName);
            String combined = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; \n" + snapSet;
            LOGGER.info("Smart snapshot: [task-{}] opening transaction with: {}", taskId, combined);
            jdbcConnection.executeWithoutCommitting(combined);
            return;
        }
        super.setSnapshotTransactionIsolationLevel(isOnDemand);
    }

    @Override
    protected void lockTablesForSchemaSnapshot(
                                               ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext) {
        LOGGER.info("Smart snapshot: [task-{}] skipping table locking (Connector holds locks)", taskId);
    }

    @Override
    protected void releaseSchemaSnapshotLocks(
                                              RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext) {
        // Signal transaction_started AFTER schema read (step 5 of doExecute).
        // Same timing as existing single-task releaseSchemaSnapshotLocks().
        // For MySQL: information_schema isn't transactional, so global lock
        // must be held during schema read. Release only after schema is captured.
        if (snapshotCoordination != null) {
            try {
                Map<String, String> taskPartition = Collect.hashMapOf(
                        "server", connectorConfig.getLogicalName(),
                        PostgresPartition.TASK_PARTITION_KEY, connectorConfig.getTaskId());
                Map<String, Object> signal = new HashMap<>();
                signal.put("transaction_started", true);
                signal.put(SmartSnapshotConnectorCoordinator.EPOCH_KEY, epoch);
                snapshotCoordination.write(taskPartition, signal);
                LOGGER.info("Smart snapshot: [task-{}] task signaled transaction_started (schema read done)", taskId);
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot: [task-{}] Failed to write transaction_started signal", taskId, e);
            }
        }
    }
}
