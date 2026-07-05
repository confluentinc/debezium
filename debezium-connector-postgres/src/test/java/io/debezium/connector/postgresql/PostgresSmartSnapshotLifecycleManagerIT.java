/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresConnection.PostgresValueConverterBuilder;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.junit.SkipTestRule;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
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

        // connector.class is required so the snapshot service providers can resolve the connector-specific impls
        io.debezium.config.Configuration config = TestHelper.defaultConfig()
                .with("connector.class", PostgresConnector.class.getName())
                .build();
        connectorConfig = new PostgresConnectorConfig(config);
        ThreadNameContext threadNameContext = ThreadNameContext.from(connectorConfig);
        TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);

        final java.nio.charset.Charset databaseCharset;
        try (PostgresConnection tmp = new PostgresConnection(connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL, threadNameContext)) {
            databaseCharset = tmp.getDatabaseCharset();
        }
        final PostgresValueConverterBuilder valueConverterBuilder = typeRegistry -> PostgresValueConverter.of(connectorConfig, databaseCharset, typeRegistry);

        connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new PostgresConnection(connectorConfig.getJdbcConfig(), valueConverterBuilder, PostgresConnection.CONNECTION_GENERAL, threadNameContext));
        jdbcConnection = connectionFactory.mainConnection();
        jdbcConnection.setAutoCommit(false);

        PostgresSchema schema = new PostgresSchema(connectorConfig, jdbcConnection.getDefaultValueConverter(),
                topicNamingStrategy, valueConverterBuilder.build(jdbcConnection.getTypeRegistry()));
        PostgresTaskContext taskContext = new PostgresTaskContext(connectorConfig, "0", schema, topicNamingStrategy);

        // register the beans + service providers the SnapshotterService needs (same set as BaseSourceTask)
        beanRegistryJdbcConnection = connectionFactory.newConnection();
        connectorConfig.getBeanRegistry().add(io.debezium.bean.StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(io.debezium.bean.StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(io.debezium.bean.StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(io.debezium.bean.StandardBeanNames.JDBC_CONNECTION, beanRegistryJdbcConnection);
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

        manager = new PostgresSmartSnapshotLifecycleManager(connectorConfig, connectionFactory, taskContext,
                snapshotterService, schema, dispatcher, notificationService, Clock.system());
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
}
