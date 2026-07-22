/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresConnection.PostgresValueConverterBuilder;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.junit.SkipTestRule;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.SmartSnapshotHeldConnectionRegistry;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager.SnapshotSetup;
import io.debezium.processors.PostProcessorRegistryServiceProvider;
import io.debezium.relational.TableId;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.SnapshotLockProvider;
import io.debezium.snapshot.SnapshotQueryProvider;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.snapshot.SnapshotterServiceProvider;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.ThreadNameContext;

/**
 * Integration test for {@link PostgresSmartSnapshotLifecycleManager} — the leader-side (task-0) mechanics
 * that create/export the replication slot, export the shared snapshot, and discover+lock the tables. Runs
 * against a live Postgres. The setup mirrors whats done in {@code PostgresConnectorTask.start()};
 * the event dispatcher and notification service are mocked because {@code discoverAndLock} does not use them.
 */
public class PostgresSmartSnapshotLifecycleManagerIT {

    @Rule
    public final SkipTestRule skip = new SkipTestRule();

    private static final String SLOT = "debezium"; // TestHelper's default replication slot name

    private PostgresConnectorConfig connectorConfig;
    private MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory;
    private PostgresConnection jdbcConnection;
    private PostgresConnection beanRegistryJdbcConnection;
    private PostgresSmartSnapshotLifecycleManager manager;

    @Before
    public void before() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE SCHEMA s1;");
        TestHelper.execute("CREATE TABLE s1.a (id int PRIMARY KEY, v text);"
                + "INSERT INTO s1.a VALUES (1, 'a1'), (2, 'a2');"
                + "CREATE TABLE s1.b (id int PRIMARY KEY, v text);"
                + "INSERT INTO s1.b VALUES (1, 'b1');");

        manager = buildManager(TestHelper.defaultConfig()
                .with("connector.class", PostgresConnector.class.getName())
                .build());
    }

    @After
    public void after() {
        if (manager != null) {
            manager.releaseSnapshot();
        }
        closeQuietly(jdbcConnection);
        closeQuietly(beanRegistryJdbcConnection);
        TestHelper.dropDefaultReplicationSlot();
    }

    @Test
    public void newSlotPathCreatesAndPersistsTheSlot() {
        assertThat(slotExists(SLOT)).isFalse();

        SnapshotSetup setup = manager.prepareSnapshot(true);

        // the exported snapshot name + consistent point must be populated (SlotCreateOrExportResult bug guard)
        assertThat(setup.snapshotName()).isNotBlank();
        assertThat(setup.consistentPosition()).isNotBlank();
        assertThat(setup.tables()).anyMatch(t -> "s1".equals(t.schema()) && "a".equals(t.table()));
        // a new logical slot was created
        assertThat(slotExists(SLOT)).isTrue();

        // releasing the leader's connections must NOT drop the slot (dropSlotOnClose=false), so streaming can resume
        manager.releaseSnapshot();
        assertThat(slotExists(SLOT)).isTrue();
    }

    @Test
    public void existingSlotPathExportsWithoutCreatingANewSlot() {
        TestHelper.createDefaultReplicationSlot();
        assertThat(slotExists(SLOT)).isTrue();

        SnapshotSetup setup = manager.prepareSnapshot(true);

        assertThat(setup.snapshotName()).isNotBlank();
        assertThat(setup.consistentPosition()).isNotBlank();
        assertThat(setup.tables()).anyMatch(t -> "s1".equals(t.schema()) && "a".equals(t.table()));
        // still exactly the one pre-existing slot; the leader exported from it rather than creating another
        assertThat(slotCount(SLOT)).isEqualTo(1);
    }

    // INITIAL mode on a pre-existing slot must start streaming at the snapshot's consistent point
    // (current WAL), not at the slot's stale confirmed_flush_lsn. Here the slot is created and then WAL
    // is advanced with inserts nobody consumes, so its confirmed_flush_lsn falls behind the current WAL.
    // The buggy code returned slotLastFlushedLsn (behind the snapshot) which would replay pre-snapshot WAL.
    @Test
    public void existingSlotInInitialModeAnchorsAtSnapshotPointNotStaleSlotLsn() {
        TestHelper.createDefaultReplicationSlot();
        assertThat(slotExists(SLOT)).isTrue();

        // advance the server WAL well past the slot's confirmed_flush_lsn
        for (int i = 0; i < 50; i++) {
            TestHelper.execute("INSERT INTO s1.a VALUES (" + (100 + i) + ", 'x');");
        }

        Lsn slotFlushed = TestHelper.getDefaultReplicationSlot().slotLastFlushedLsn();

        SnapshotSetup setup = manager.prepareSnapshot(true); // default snapshot mode = INITIAL

        Lsn consistent = Lsn.valueOf(setup.consistentPosition());
        // INITIAL (shouldStreamEventsStartingFromSnapshot()==true) must anchor ahead of the stale slot LSN
        assertThat(consistent.compareTo(slotFlushed)).isGreaterThan(0);
        assertThat(slotCount(SLOT)).isEqualTo(1);
    }

    @Test
    public void nonStreamingPathExportsSnapshotWithoutASlot() {
        assertThat(slotExists(SLOT)).isFalse();

        SnapshotSetup setup = manager.prepareSnapshot(false);

        assertThat(setup.snapshotName()).isNotBlank();
        assertThat(setup.consistentPosition()).isNotBlank();
        assertThat(setup.tables()).anyMatch(t -> "s1".equals(t.schema()) && "a".equals(t.table()));
        // initial_only style: no replication slot is created
        assertThat(slotExists(SLOT)).isFalse();
    }

    @Test
    public void keepAliveSucceedsWhileConnectionsAreHeld() {
        manager.prepareSnapshot(true);

        // held snapshot/lock connections are alive -> keepAlive must not throw
        assertThatCode(() -> manager.keepAlive()).doesNotThrowAnyException();
    }

    // a held connection is busy with a long query, and
    // releaseSnapshot() on another thread must abort it rather than wait for it to finish.
    // This is the mechanism the whole fix relies on -- JdbcConnection.close() waits briefly and then abort()s a
    // busy connection, which ends the query and unblocks the thread running it. Without the abort this
    // would sit on the 60s sleep.
    @Test
    public void releaseSnapshotAbortsAnInFlightQueryOnAHeldConnection() throws Exception {
        PostgresConnection held = connectionFactory.newConnection();
        held.connection().setAutoCommit(false);
        // Register the held connection with the manager's connection registry so releaseSnapshot() closes (and
        // aborts) it, the same way the leader registers the snapshot-holder connection during prepareSnapshot().
        SmartSnapshotHeldConnectionRegistry heldConnections = (SmartSnapshotHeldConnectionRegistry) getField(manager, "heldConnections");
        heldConnections.registerConnection("snapshot holder", held);

        CountDownLatch queryStarted = new CountDownLatch(1);
        AtomicReference<Throwable> queryOutcome = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                queryStarted.countDown();
                // long blocking query that must be aborted, not run to completion
                held.executeWithoutCommitting("SELECT pg_sleep(60)");
            }
            catch (Throwable t) {
                queryOutcome.set(t);
            }
        }, "held-query");
        worker.start();

        assertThat(queryStarted.await(5, TimeUnit.SECONDS)).isTrue();
        // let the query actually start running in the backend before we abort it
        Thread.sleep(500);

        long startMs = System.currentTimeMillis();
        manager.releaseSnapshot();
        long releaseMs = System.currentTimeMillis() - startMs;

        worker.join(30_000);
        // the query was aborted (it threw), not allowed to complete its 60s sleep
        assertThat(worker.isAlive()).isFalse();
        assertThat(queryOutcome.get()).isNotNull();
        // release finished in bounded time, far below the 60s sleep
        assertThat(releaseMs).isLessThan(20_000);
    }

    // a real prepareSnapshot() is blocked mid-flight on a table lock, and a
    // concurrent releaseSnapshot() (the task-stop path) must abort it rather than wait the lock out.
    // 'shared' locking makes the leader take ACCESS SHARE on the tables during discoverAndLock; an
    // ACCESS EXCLUSIVE lock held by another session blocks that. releaseSnapshot closes the leader's
    // held connection, which aborts the blocked lock wait so the preparation thread ends.
    @Test
    public void releaseSnapshotAbortsAnInFlightPrepareBlockedOnATableLock() throws Exception {
        // rebuild the manager with shared locking; release/close the default-config one from before()
        manager.releaseSnapshot();
        closeQuietly(jdbcConnection);
        closeQuietly(beanRegistryJdbcConnection);
        manager = buildManager(TestHelper.defaultConfig()
                .with("connector.class", PostgresConnector.class.getName())
                .with(PostgresConnectorConfig.SNAPSHOT_LOCKING_MODE, "shared")
                .build());

        PostgresConnection blocker = connectionFactory.newConnection();
        try {
            // hold ACCESS EXCLUSIVE on s1.a so the leader's ACCESS SHARE lock blocks
            blocker.connection().setAutoCommit(false);
            blocker.executeWithoutCommitting("LOCK TABLE s1.a IN ACCESS EXCLUSIVE MODE");

            AtomicReference<Throwable> prepareOutcome = new AtomicReference<>();
            Thread prepThread = new Thread(() -> {
                try {
                    manager.prepareSnapshot(true);
                }
                catch (Throwable t) {
                    prepareOutcome.set(t);
                }
            }, "leader-prep");
            prepThread.start();

            // wait until the leader is actually parked waiting for the table lock
            assertThat(waitForBackendBlockedOnLock(20)).isTrue();

            // stop concurrently: this must abort the blocked lock wait, not hang behind it
            long startMs = System.currentTimeMillis();
            manager.releaseSnapshot();
            long releaseMs = System.currentTimeMillis() - startMs;

            prepThread.join(30_000);
            // preparation unblocked and ended
            assertThat(prepThread.isAlive()).isFalse();
            // it was aborted, did not publish a snapshot
            assertThat(prepareOutcome.get()).isNotNull();
            // release did not wait the lock out
            assertThat(releaseMs).isLessThan(20_000);
        }
        finally {
            try {
                blocker.connection().rollback();
            }
            catch (Exception ignored) {
                // best effort
            }
            closeQuietly(blocker);
        }
    }

    private boolean waitForBackendBlockedOnLock(int seconds) throws Exception {
        for (int i = 0; i < seconds * 10; i++) {
            try (PostgresConnection c = connectionFactory.newConnection()) {
                boolean blocked = c.queryAndMap(
                        "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'",
                        rs -> {
                            rs.next();
                            return rs.getInt(1) > 0;
                        });
                if (blocked) {
                    return true;
                }
            }
            Thread.sleep(100);
        }
        return false;
    }

    private boolean slotExists(String name) {
        return slotCount(name) > 0;
    }

    private int slotCount(String name) {
        try (PostgresConnection c = connectionFactory.newConnection()) {
            return c.queryAndMap("SELECT count(*) FROM pg_replication_slots WHERE slot_name = '" + name + "'", rs -> {
                rs.next();
                return rs.getInt(1);
            });
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void closeQuietly(PostgresConnection c) {
        if (c != null) {
            try {
                c.close();
            }
            catch (Exception ignored) {
                // best effort
            }
        }
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = PostgresSmartSnapshotLifecycleManager.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private PostgresSmartSnapshotLifecycleManager buildManager(io.debezium.config.Configuration config) throws Exception {
        connectorConfig = new PostgresConnectorConfig(config);
        ThreadNameContext threadNameContext = ThreadNameContext.from(connectorConfig);
        TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);

        final java.nio.charset.Charset databaseCharset;
        try (PostgresConnection tmp = new PostgresConnection(connectorConfig.getJdbcConfig(),
                PostgresConnection.CONNECTION_GENERAL, threadNameContext)) {
            databaseCharset = tmp.getDatabaseCharset();
        }
        final PostgresValueConverterBuilder valueConverterBuilder = typeRegistry -> PostgresValueConverter.of(connectorConfig, databaseCharset, typeRegistry);

        connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new PostgresConnection(connectorConfig.getJdbcConfig(),
                        valueConverterBuilder, PostgresConnection.CONNECTION_GENERAL, threadNameContext));
        jdbcConnection = connectionFactory.mainConnection();
        jdbcConnection.setAutoCommit(false);

        PostgresSchema schema = new PostgresSchema(connectorConfig,
                jdbcConnection.getDefaultValueConverter(),
                topicNamingStrategy,
                valueConverterBuilder.build(jdbcConnection.getTypeRegistry()));
        PostgresTaskContext taskContext = new PostgresTaskContext(connectorConfig, "0", schema,
                topicNamingStrategy);

        beanRegistryJdbcConnection = connectionFactory.newConnection();
        connectorConfig.getBeanRegistry().add(io.debezium.bean.StandardBeanNames.CONFIGURATION,
                config);

        connectorConfig.getBeanRegistry().add(io.debezium.bean.StandardBeanNames.CONNECTOR_CONFIG,
                connectorConfig);

        connectorConfig.getBeanRegistry().add(io.debezium.bean.StandardBeanNames.DATABASE_SCHEMA,
                schema);

        connectorConfig.getBeanRegistry().add(io.debezium.bean.StandardBeanNames.JDBC_CONNECTION,
                beanRegistryJdbcConnection);
        ServiceRegistry serviceRegistry = connectorConfig.getServiceRegistry();
        serviceRegistry.registerServiceProvider(new PostProcessorRegistryServiceProvider());
        serviceRegistry.registerServiceProvider(new SnapshotLockProvider());
        serviceRegistry.registerServiceProvider(new SnapshotQueryProvider());
        serviceRegistry.registerServiceProvider(new SnapshotterServiceProvider());
        SnapshotterService snapshotterService = serviceRegistry.tryGetService(SnapshotterService.class);

        @SuppressWarnings("unchecked")
        EventDispatcher<PostgresPartition, TableId> dispatcher = mock(EventDispatcher.class);
        @SuppressWarnings("unchecked")
        NotificationService<PostgresPartition, PostgresOffsetContext> notificationService = mock(NotificationService.class);

        return new PostgresSmartSnapshotLifecycleManager(connectorConfig, connectionFactory,
                taskContext,
                snapshotterService, schema, dispatcher, notificationService, Clock.system(), 1);
    }
}
