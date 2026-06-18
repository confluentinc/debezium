/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordination;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Multi-task ("smart snapshot") variant of {@link SqlServerSnapshotChangeEventSource}.
 * <p>
 * The Connector (via {@code SmartSnapshotConnectorCoordinator} + {@link SqlServerSnapshotLifecycleManager})
 * holds the {@code TABLOCKX} write barrier and has captured the consistent position {@code L_db}. This task
 * therefore:
 * <ul>
 * <li>uses {@code L_db} (read from the coordination topic, supplied via {@link #setSmartSnapshotLsn}) as its
 * snapshot offset instead of capturing a fresh max LSN;</li>
 * <li>opens its own {@code snapshot}-isolation transaction (handled by the base {@code connectionCreated}
 * when {@code snapshot.isolation.mode=snapshot}, which smart snapshot requires) — SQL Server has no
 * exportable snapshot to join by name;</li>
 * <li>skips table locking (the Connector holds the barrier);</li>
 * <li>signals {@code transaction_started} once its schema read is done, so the Connector can release the
 * barrier ({@code onAllTasksJoined}).</li>
 * </ul>
 */
public class SqlServerSmartSnapshotChangeEventSource extends SqlServerSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSmartSnapshotChangeEventSource.class);

    private final SqlServerConnectorConfig connectorConfig;
    private final String taskId;

    private String smartSnapshotLsn;
    private SnapshotCoordination snapshotCoordination;
    private int epoch;

    public SqlServerSmartSnapshotChangeEventSource(SqlServerConnectorConfig connectorConfig,
                                                   MainConnectionProvidingConnectionFactory<SqlServerConnection> connectionFactory,
                                                   SqlServerDatabaseSchema schema, EventDispatcher<SqlServerPartition, TableId> dispatcher, Clock clock,
                                                   SnapshotProgressListener<SqlServerPartition> snapshotProgressListener,
                                                   NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService,
                                                   SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.taskId = connectorConfig.getTaskId();
    }

    public void setSmartSnapshotLsn(String lsn) {
        this.smartSnapshotLsn = lsn;
    }

    public void setSnapshotCoordination(SnapshotCoordination coordination, int epoch) {
        this.snapshotCoordination = coordination;
        this.epoch = epoch;
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> ctx,
                                           SqlServerOffsetContext previousOffset)
            throws Exception {
        // Use the Connector's consistent position (L_db), not a fresh max LSN.
        SqlServerOffsetContext offset = new SqlServerOffsetContext(
                connectorConfig,
                TxLogPosition.valueOf(Lsn.valueOf(smartSnapshotLsn)),
                null,
                false);
        offset.setEpoch(epoch);
        ctx.offset = offset;
        LOGGER.info("Smart snapshot [task-{}]: snapshot offset set to L_db={}, epoch={}", taskId, smartSnapshotLsn, epoch);
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext) {
        LOGGER.info("Smart snapshot [task-{}]: skipping table locking (Connector holds the TABLOCKX barrier)", taskId);
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext)
            throws java.sql.SQLException {
        // Schema has been read; signal the Connector that this task has joined the snapshot (opened its
        // snapshot-isolation transaction), so the Connector can release the write barrier once all join.
        if (snapshotCoordination != null) {
            try {
                Map<String, Object> signal = new HashMap<>();
                signal.put("transaction_started", true);
                signal.put(SmartSnapshotConnectorCoordinator.EPOCH_KEY, epoch);
                snapshotCoordination.write(snapshotContext.partition.getSourcePartition(), signal);
                LOGGER.info("Smart snapshot [task-{}]: signaled transaction_started (schema read done)", taskId);
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot [task-{}]: failed to write transaction_started signal", taskId, e);
            }
        }
        super.releaseSchemaSnapshotLocks(snapshotContext);
    }
}
