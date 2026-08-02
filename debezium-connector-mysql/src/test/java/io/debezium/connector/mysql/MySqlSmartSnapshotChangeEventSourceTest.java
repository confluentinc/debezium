/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogOffsetContext;
import io.debezium.connector.binlog.BinlogSourceInfo;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Unit tests for {@link MySqlSmartSnapshotChangeEventSource}. These cover the overridden hooks in isolation
 * (no database), using the leader-published snapshot slice and consistent point injected via
 * {@link MySqlSmartSnapshotChangeEventSource#setSmartSnapshot}.
 *
 * <p>Unlike Postgres — which imports the leader's exported snapshot with {@code SET TRANSACTION SNAPSHOT} — MySQL
 * has no exportable snapshot. A follower instead opens its own {@code START TRANSACTION WITH CONSISTENT SNAPSHOT}
 * while the leader holds the global write lock, and stamps its offset with the shared binlog position {@code P}
 * (file/pos/gtids) from the coordination topic rather than its own {@code SHOW MASTER STATUS}. Schema is applied
 * in memory only, since the leader task is the single writer of the schema history.
 */
public class MySqlSmartSnapshotChangeEventSourceTest {

    private static final String TASK_ID = "1";
    private static final int EPOCH = 5;
    private static final String BINLOG_FILE = "mysql-bin.000003";
    private static final long BINLOG_POS = 456L;
    private static final String GTID_SET = "123e4567-e89b-12d3-a456-426614174000:1-10";

    private static final TableId TABLE_A = new TableId("db", null, "a");
    private static final TableId TABLE_B = new TableId("db", null, "b");

    @Mock
    private MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory;

    @Mock
    private BinlogConnectorConnection jdbcConnection;

    @Mock
    private Connection sqlConnection;

    @Mock
    private MySqlDatabaseSchema schema;

    @Mock
    private EventDispatcher<MySqlPartition, TableId> dispatcher;

    @Mock
    private MySqlSnapshotChangeEventSourceMetrics metrics;

    @Mock
    private NotificationService<MySqlPartition, MySqlOffsetContext> notificationService;

    @Mock
    private SnapshotterService snapshotterService;

    @Mock
    private SnapshotCoordinationFacade coordination;

    private MySqlConnectorConfig connectorConfig;
    private MySqlSmartSnapshotChangeEventSource source;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);
        when(connectionFactory.mainConnection()).thenReturn(jdbcConnection);

        Configuration config = Configuration.create()
                .with(BinlogConnectorConfig.HOSTNAME, "localhost")
                .with(BinlogConnectorConfig.USER, "user")
                .with(BinlogConnectorConfig.PASSWORD, "pass")
                // satisfies the JDBC credentials provider during config construction
                .with("jdbc.creds.provider.user", "user")
                .with("jdbc.creds.provider.password", "pass")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "test_server")
                .with(ConfigurationNames.TASK_ID_PROPERTY_NAME, TASK_ID)
                .build();
        connectorConfig = new MySqlConnectorConfig(config);

        source = new MySqlSmartSnapshotChangeEventSource(connectorConfig, connectionFactory, schema, dispatcher,
                Clock.system(), metrics, null, null, notificationService, snapshotterService);
    }

    private RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> newContext() {
        return new RelationalSnapshotContext<>(new MySqlPartition("test_server", "db", TASK_ID), "db", false);
    }

    @Test
    public void determineCapturedTablesUsesTheLeaderPublishedSlice() throws Exception {
        source.setSmartSnapshot(EPOCH, BINLOG_FILE, BINLOG_POS, GTID_SET, List.of(TABLE_A, TABLE_B), coordination);
        RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> ctx = newContext();

        // the ignored-patterns and snapshotting-task args are unused on the smart path
        source.determineCapturedTables(ctx, Collections.emptySet(), null);

        assertThat(ctx.capturedTables).containsExactly(TABLE_A, TABLE_B);
        assertThat(ctx.capturedSchemaTables).containsExactly(TABLE_A, TABLE_B);
    }

    @Test
    public void determineSnapshotOffsetAnchorsToTheLeaderConsistentPoint() throws Exception {
        source.setSmartSnapshot(EPOCH, BINLOG_FILE, BINLOG_POS, GTID_SET, List.of(), coordination);
        RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> ctx = newContext();

        source.determineSnapshotOffset(ctx, null);

        assertThat(ctx.offset).isNotNull();
        // The offset must reflect the shared P from the coordination topic (not this connection's own position),
        // and carry the smart-snapshot epoch so a restart can detect a stale round.
        assertThat(ctx.offset.getOffset().get(BinlogSourceInfo.BINLOG_FILENAME_OFFSET_KEY)).isEqualTo(BINLOG_FILE);
        assertThat(ctx.offset.getOffset().get(BinlogSourceInfo.BINLOG_POSITION_OFFSET_KEY)).isEqualTo(BINLOG_POS);
        assertThat(ctx.offset.getOffset().get(BinlogOffsetContext.GTID_SET_KEY)).isEqualTo(GTID_SET);
        assertThat(ctx.offset.getOffset().get(SnapshotCoordinationFacade.EPOCH)).isEqualTo(EPOCH);
    }

    @Test
    public void lockTablesForSchemaSnapshotOpensAConsistentSnapshotTransactionAndTakesNoLock() throws Exception {
        when(jdbcConnection.connection()).thenReturn(sqlConnection);
        source.setSmartSnapshot(EPOCH, BINLOG_FILE, BINLOG_POS, GTID_SET, List.of(), coordination);

        source.lockTablesForSchemaSnapshot(null, newContext());

        // The follower takes NO table/global lock; it only freezes its reads at P via a consistent-snapshot
        // transaction opened while the leader's write lock is held.
        verify(sqlConnection).setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        verify(jdbcConnection).executeWithoutCommitting("START TRANSACTION WITH CONSISTENT SNAPSHOT");
    }

    @Test
    public void twoPhaseSchemaSnapshotIsDisabled() {
        // The follower is not globally locked and must NOT take its own table locks / two-phase snapshot.
        assertThat(source.twoPhaseSchemaSnapshot()).isFalse();
    }

    @Test
    public void emitSchemaChangeEventAppliesSchemaInMemoryOnly() {
        source.setSmartSnapshot(EPOCH, BINLOG_FILE, BINLOG_POS, GTID_SET, List.of(), coordination);
        SchemaChangeEvent event = org.mockito.Mockito.mock(SchemaChangeEvent.class);

        source.emitSchemaChangeEvent(newContext(), event, TABLE_A);

        // Single-writer schema history: a follower applies the schema in memory only — no history persist and no
        // public schema-change topic. Only the leader task writes the schema history.
        verify(schema).applySchemaChangeInMemoryOnly(event);
    }

    @Test
    public void releaseSchemaSnapshotLocksSignalsTransactionStarted() {
        source.setSmartSnapshot(EPOCH, BINLOG_FILE, BINLOG_POS, GTID_SET, List.of(), coordination);

        source.releaseSchemaSnapshotLocks(newContext());

        verify(coordination).writeTaskStartedTransaction(TASK_ID, EPOCH);
    }

    @Test
    public void releaseSchemaSnapshotLocksDoesNotSwallowCoordinationErrors() {
        source.setSmartSnapshot(EPOCH, BINLOG_FILE, BINLOG_POS, GTID_SET, List.of(), coordination);
        doThrow(new RuntimeException("topic down")).when(coordination).writeTaskStartedTransaction(anyString(), anyInt());

        assertThatCode(() -> source.releaseSchemaSnapshotLocks(newContext())).hasMessage("topic down");
        verify(coordination).writeTaskStartedTransaction(TASK_ID, EPOCH);
    }
}
