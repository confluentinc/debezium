/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.connector.mysql.jdbc.MySqlConnection;
import io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration;
import io.debezium.connector.mysql.jdbc.MySqlFieldReaderResolver;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.junit.SkipTestRule;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.SmartSnapshotHeldConnectionRegistry;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager.SnapshotSetup;
import io.debezium.processors.PostProcessorRegistryServiceProvider;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.SnapshotLockProvider;
import io.debezium.snapshot.SnapshotQueryProvider;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.snapshot.SnapshotterServiceProvider;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.Testing;
import io.debezium.util.ThreadNameContext;

/**
 * Integration test for {@link MySqlSmartSnapshotLifecycleManager} — the leader-side (task-0) mechanics that
 * acquire the write-blocking lock, capture the binlog position {@code P}, enumerate the captured tables, hold the
 * lock until release, and abort in-flight work on stop. Runs against a live MySQL. The dispatcher/notification
 * service are mocked (the schema-history persistence they drive is covered by the engine IT); this focuses on the
 * lock/position mechanics unique to MySQL.
 */
public class MySqlSmartSnapshotLifecycleManagerIT {

    @Rule
    public final SkipTestRule skip = new SkipTestRule();

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-smart-snapshot-lcm.txt")
            .toAbsolutePath();

    private final UniqueDatabase DATABASE = new MySqlUniqueDatabase("lcm", "smart_snapshot_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory;
    private BinlogConnectorConnection jdbcConnection;
    private MySqlDatabaseSchema schema;
    private MySqlSmartSnapshotLifecycleManager manager;

    @Before
    public void before() throws Exception {
        DATABASE.createAndInitialize();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        manager = buildManager(DATABASE.defaultConfig()
                .with("connector.class", MySqlConnector.class.getName())
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL.getValue())
                .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.MINIMAL)
                .build());
    }

    @After
    public void after() {
        if (manager != null) {
            manager.releaseSnapshot();
        }
        if (schema != null) {
            schema.close(); // unregister schema/schema-history JMX MBeans so the next test doesn't collide
        }
        closeQuietly(jdbcConnection);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @Test
    public void prepareSnapshotCapturesConsistentPointAndTables() {
        SnapshotSetup setup = manager.prepareSnapshot(true);

        // MySQL has no exported snapshot name; the shared point is the encoded binlog position file:pos:gtids
        assertThat(setup.snapshotName()).isNull();
        assertThat(setup.consistentPosition()).isNotBlank();
        assertThat(setup.consistentPosition()).contains(":");
        assertThat(setup.tables()).anyMatch(t -> "a".equals(t.table()));
        assertThat(setup.tables()).anyMatch(t -> "b".equals(t.table()));
    }

    @Test
    public void leaderHoldsGlobalWriteLockUntilRelease() throws Exception {
        manager.prepareSnapshot(true); // acquires FLUSH TABLES WITH READ LOCK, held until release

        // a write from another connection must block while the lock is held
        AtomicReference<Throwable> writeOutcome = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try (MySqlTestConnection w = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
                w.execute("INSERT INTO a VALUES (500, 'after-unlock')");
            }
            catch (Throwable t) {
                writeOutcome.set(t);
            }
        }, "blocked-writer");
        writer.start();

        writer.join(2000);
        assertThat(writer.isAlive()).as("write should be blocked while the leader holds the global read lock").isTrue();

        // releasing the leader's lock connection releases the lock; the write must then complete
        manager.releaseSnapshot();
        writer.join(15_000);
        assertThat(writer.isAlive()).as("write should complete once the lock is released").isFalse();
        assertThat(writeOutcome.get()).isNull();
        assertThat(rowExists("a", 500)).isTrue();
    }

    @Test
    public void keepAliveSucceedsWhileLockHeld() {
        manager.prepareSnapshot(true);
        assertThatCode(() -> manager.keepAlive()).doesNotThrowAnyException();
    }

    // A held connection is busy with a long query; releaseSnapshot() on another thread must abort it rather than
    // wait it out. This is the mechanism the stop path relies on (close() aborts a busy connection).
    @Test
    public void releaseSnapshotAbortsAnInFlightQueryOnAHeldConnection() throws Exception {
        BinlogConnectorConnection held = connectionFactory.newConnection();
        held.connection().setAutoCommit(false);
        SmartSnapshotHeldConnectionRegistry heldConnections = (SmartSnapshotHeldConnectionRegistry) getField(manager, "heldConnections");
        heldConnections.registerConnection("held", held);

        CountDownLatch queryStarted = new CountDownLatch(1);
        AtomicReference<Throwable> queryOutcome = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                queryStarted.countDown();
                held.executeWithoutCommitting("SELECT SLEEP(60)");
            }
            catch (Throwable t) {
                queryOutcome.set(t);
            }
        }, "held-query");
        worker.start();

        assertThat(queryStarted.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(500);

        long startMs = System.currentTimeMillis();
        manager.releaseSnapshot();
        long releaseMs = System.currentTimeMillis() - startMs;

        worker.join(30_000);
        assertThat(worker.isAlive()).isFalse();
        assertThat(queryOutcome.get()).as("the in-flight query should have been aborted").isNotNull();
        assertThat(releaseMs).as("release should not wait out the 60s sleep").isLessThan(20_000);
    }

    private boolean rowExists(String table, int id) {
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            return db.queryAndMap("SELECT COUNT(*) FROM " + table + " WHERE id = " + id, rs -> {
                rs.next();
                return rs.getInt(1) > 0;
            });
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void closeQuietly(BinlogConnectorConnection c) {
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
        Field field = MySqlSmartSnapshotLifecycleManager.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private MySqlSmartSnapshotLifecycleManager buildManager(Configuration config) throws Exception {
        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);
        ThreadNameContext threadNameContext = ThreadNameContext.from(connectorConfig);
        TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(MySqlConnectorConfig.TOPIC_NAMING_STRATEGY);
        SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        MySqlValueConverters valueConverters = new MySqlValueConverters(
                connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(),
                connectorConfig.getBigIntUnsignedHandlingMode().asBigIntUnsignedMode(),
                connectorConfig.binaryHandlingMode(),
                connectorConfig.isTimeAdjustedEnabled() ? MySqlValueConverters::adjustTemporal : x -> x,
                connectorConfig.getEventConvertingFailureHandlingMode(),
                connectorConfig.getServiceRegistry());

        connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new MySqlConnection(new MySqlConnectionConfiguration(config),
                        MySqlFieldReaderResolver.resolve(connectorConfig), threadNameContext));
        jdbcConnection = connectionFactory.mainConnection();
        final boolean tableIdCaseInsensitive = jdbcConnection.isTableIdCaseSensitive();

        schema = new MySqlDatabaseSchema(connectorConfig, valueConverters, topicNamingStrategy,
                schemaNameAdjuster, tableIdCaseInsensitive);
        MySqlTaskContext taskContext = new MySqlTaskContext(connectorConfig, schema);

        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, jdbcConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);

        ServiceRegistry serviceRegistry = connectorConfig.getServiceRegistry();
        serviceRegistry.registerServiceProvider(new PostProcessorRegistryServiceProvider());
        serviceRegistry.registerServiceProvider(new SnapshotLockProvider());
        serviceRegistry.registerServiceProvider(new SnapshotQueryProvider());
        serviceRegistry.registerServiceProvider(new SnapshotterServiceProvider());
        SnapshotterService snapshotterService = serviceRegistry.tryGetService(SnapshotterService.class);

        @SuppressWarnings("unchecked")
        EventDispatcher<MySqlPartition, TableId> dispatcher = mock(EventDispatcher.class);
        MySqlSnapshotChangeEventSourceMetrics metrics = mock(MySqlSnapshotChangeEventSourceMetrics.class);
        @SuppressWarnings("unchecked")
        NotificationService<MySqlPartition, MySqlOffsetContext> notificationService = mock(NotificationService.class);

        return new MySqlSmartSnapshotLifecycleManager(connectorConfig, connectionFactory, schema, dispatcher,
                Clock.system(), metrics, notificationService, snapshotterService, 1);
    }
}
