/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Unit tests for {@link PostgresSmartSnapshotChangeEventSource}. These cover the four overridden hooks in
 * isolation (no database), using the leader-published snapshot slice injected via
 * {@link PostgresSmartSnapshotChangeEventSource#setSnapshotCoordination}.
 */
public class PostgresSmartSnapshotChangeEventSourceTest {

    private static final String TASK_ID = "1";
    private static final int EPOCH = 5;
    private static final String SNAPSHOT_NAME = "00000003-0000001B-1";

    private static final TableId TABLE_A = new TableId(null, "public", "a");
    private static final TableId TABLE_B = new TableId(null, "public", "b");

    @Mock
    private MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory;

    @Mock
    private PostgresConnection jdbcConnection;

    @Mock
    private PostgresSchema schema;

    @Mock
    private EventDispatcher<PostgresPartition, TableId> dispatcher;

    @Mock
    private NotificationService<PostgresPartition, PostgresOffsetContext> notificationService;

    @Mock
    private SnapshotterService snapshotterService;

    @Mock
    private SnapshotCoordinationFacade coordination;

    private PostgresConnectorConfig connectorConfig;
    private PostgresSmartSnapshotChangeEventSource source;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);
        when(connectionFactory.mainConnection()).thenReturn(jdbcConnection);

        Configuration config = Configuration.create()
                .with(PostgresConnectorConfig.HOSTNAME, "localhost")
                .with(PostgresConnectorConfig.PORT, 5432)
                .with(PostgresConnectorConfig.USER, "user")
                .with(PostgresConnectorConfig.PASSWORD, "pass")
                .with(PostgresConnectorConfig.DATABASE_NAME, "db")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "test_server")
                .with(ConfigurationNames.TASK_ID_PROPERTY_NAME, TASK_ID)
                .build();
        connectorConfig = new PostgresConnectorConfig(config);

        source = new PostgresSmartSnapshotChangeEventSource(connectorConfig, snapshotterService, connectionFactory,
                schema, dispatcher, Clock.system(), SnapshotProgressListener.NO_OP(), null, null, notificationService);
    }

    private RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> newContext() {
        return new RelationalSnapshotContext<>(new PostgresPartition("test_server", "db", TASK_ID), "db", false);
    }

    @Test
    public void determineCapturedTablesUsesTheLeaderPublishedSlice() throws Exception {
        source.setSnapshotCoordination(EPOCH, SNAPSHOT_NAME, null, List.of(TABLE_A, TABLE_B), coordination);
        RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx = newContext();

        // the ignored-patterns and snapshotting-task args are unused on the smart path
        source.determineCapturedTables(ctx, Collections.emptySet(), null);

        assertThat(ctx.capturedTables).containsExactly(TABLE_A, TABLE_B);
        assertThat(ctx.capturedSchemaTables).containsExactly(TABLE_A, TABLE_B);
    }

    @Test
    public void setSnapshotTransactionIsolationLevelImportsTheExportedSnapshot() throws Exception {
        source.setSnapshotCoordination(EPOCH, SNAPSHOT_NAME, null, List.of(), coordination);

        source.setSnapshotTransactionIsolationLevel(false);

        verify(jdbcConnection).executeWithoutCommitting(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; \nSET TRANSACTION SNAPSHOT '" + SNAPSHOT_NAME + "';");
    }

    @Test
    public void setSnapshotTransactionIsolationLevelFallsBackToSuperWhenOnDemand() throws Exception {
        source.setSnapshotCoordination(EPOCH, SNAPSHOT_NAME, null, List.of(), coordination);

        source.setSnapshotTransactionIsolationLevel(true);

        verify(jdbcConnection, never()).executeWithoutCommitting(contains("SET TRANSACTION SNAPSHOT"));
    }

    @Test
    public void setSnapshotTransactionIsolationLevelFallsBackToSuperWhenNoSnapshotName() throws Exception {
        // no setSnapshotCoordination() call -> smartSnapshotName is null
        source.setSnapshotTransactionIsolationLevel(false);

        verify(jdbcConnection, never()).executeWithoutCommitting(contains("SET TRANSACTION SNAPSHOT"));
    }

    @Test
    public void releaseSchemaSnapshotLocksSignalsTransactionStarted() {
        source.setSnapshotCoordination(EPOCH, SNAPSHOT_NAME, null, List.of(), coordination);

        source.releaseSchemaSnapshotLocks(newContext());

        verify(coordination).writeTransactionStarted(TASK_ID, EPOCH);
    }

    @Test
    public void releaseSchemaSnapshotLocksSwallowsCoordinationErrors() {
        source.setSnapshotCoordination(EPOCH, SNAPSHOT_NAME, null, List.of(), coordination);
        doThrow(new RuntimeException("topic down")).when(coordination).writeTransactionStarted(anyString(), anyInt());

        // best-effort: a failed signal must not break the (already finished) schema read
        assertThatCode(() -> source.releaseSchemaSnapshotLocks(newContext())).doesNotThrowAnyException();
        verify(coordination).writeTransactionStarted(TASK_ID, EPOCH);
    }

    @Test
    public void determineSnapshotOffsetUsesTheLeaderSlotLsn() throws Exception {
        when(jdbcConnection.currentXLogLocation()).thenReturn(0L);
        when(jdbcConnection.currentTransactionId()).thenReturn(42L);
        Lsn slotLsn = Lsn.valueOf("0/16B3748");
        source.setSnapshotCoordination(EPOCH, SNAPSHOT_NAME, slotLsn, List.of(), coordination);
        RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx = newContext();

        source.determineSnapshotOffset(ctx, null);

        assertThat(ctx.offset).isNotNull();
        assertThat(ctx.offset.getOffset().get(SourceInfo.LSN_KEY)).isEqualTo(slotLsn.asLong());
    }
}
