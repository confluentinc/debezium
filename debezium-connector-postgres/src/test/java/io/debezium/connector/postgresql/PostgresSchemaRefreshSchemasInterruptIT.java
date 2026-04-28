/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Integration test that asserts {@link PostgresSchema#refreshSchemas()} responds to
 * {@link Thread#interrupt()} while iterating tables.
 *
 * <p>Without the interrupt check in {@code refreshSchemas()}, the per-table CPU loop
 * (schema build + OpenLineage emit arg-building) runs to completion ignoring shutdown
 * signals — the bug that produced zombie coordinator threads on rebalance during
 * initial schema load (reproduced in devel as connector lcc-devc0xxm7wp).
 *
 * <p>How the test works:
 * <ol>
 *   <li>Construct a {@link TestPostgresSchema} (test subclass that exposes
 *       {@code tables()} for in-memory population)</li>
 *   <li>Pre-populate {@code tables()} with N fake in-memory {@link Table} objects
 *       (no DB queries needed — the loop only touches in-memory state)</li>
 *   <li>Spawn a worker thread that calls {@code schema.refreshSchemas()}</li>
 *   <li>Interrupt the worker mid-loop</li>
 *   <li>Assert: worker exits within 1 second with {@link ConnectException}</li>
 * </ol>
 *
 * @author Yashi Srivastava
 */
public class PostgresSchemaRefreshSchemasInterruptIT extends AbstractRecordsProducerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSchemaRefreshSchemasInterruptIT.class);

    /**
     * Enough fake tables that refreshSchemas() takes ~3-5 seconds, giving a reliable
     * mid-loop window for the interrupt to land.
     */
    private static final int FAKE_TABLE_COUNT = 10_000;

    private PostgresConnection connection;
    private TestPostgresSchema schema;

    @Before
    public void before() throws Exception {
        TestHelper.dropAllSchemas();
        initializeConnectorTestFramework();

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "test")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "test\\..*");
        PostgresConnectorConfig config = new PostgresConnectorConfig(configBuilder.build());

        connection = TestHelper.createWithTypeRegistry();
        schema = new TestPostgresSchema(config, connection);

        long t0 = System.currentTimeMillis();
        for (int i = 0; i < FAKE_TABLE_COUNT; i++) {
            schema.addFakeTable(makeFakeTable(i));
        }
        LOGGER.info("Pre-populated {} fake tables in {}ms",
                FAKE_TABLE_COUNT, System.currentTimeMillis() - t0);
    }

    @After
    public void after() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void refreshSchemasShouldExitWhenInterrupted() throws Exception {
        AtomicReference<Throwable> caught = new AtomicReference<>();

        Thread worker = new Thread(() -> {
            try {
                LOGGER.info("Worker starting refreshSchemas() over {} tables", FAKE_TABLE_COUNT);
                schema.refreshSchemas();
                LOGGER.warn("Worker finished refreshSchemas() normally (unexpected — should have been interrupted)");
            }
            catch (Throwable t) {
                caught.set(t);
                LOGGER.info("Worker exited with {}: {}", t.getClass().getSimpleName(), t.getMessage());
            }
        }, "test-refresh-worker");

        worker.start();

        // Let the loop get well into the iteration before interrupting
        Thread.sleep(500);
        assertThat(worker.isAlive())
                .as("Worker should still be inside refreshSchemas() loop")
                .isTrue();

        long interruptAt = System.currentTimeMillis();
        LOGGER.info("Sending interrupt to worker");
        worker.interrupt();
        worker.join(5_000);
        long responseMs = System.currentTimeMillis() - interruptAt;

        // Assertions — these would all fail against the pre-fix code
        assertThat(worker.isAlive())
                .as("Worker should exit after interrupt — without the refreshSchemas() interrupt check, "
                        + "the loop would run to completion ignoring shutdown")
                .isFalse();

        assertThat(responseMs)
                .as("Worker should exit within 1 second of interrupt; actual=%dms. "
                        + "If this is much higher, the per-iteration interrupt check in refreshSchemas() "
                        + "is missing or misplaced.", responseMs)
                .isLessThan(1000);

        assertThat(caught.get())
                .as("Worker should propagate ConnectException on interrupt")
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("interrupted");

        LOGGER.info("Test passed: worker exited {}ms after interrupt with ConnectException", responseMs);
    }

    private static Table makeFakeTable(int index) {
        return Table.editor()
                .tableId(new TableId(null, "test", String.format("t_%05d", index)))
                .addColumns(
                        Column.editor().name("id").type("INT").jdbcType(Types.INTEGER).create(),
                        Column.editor().name("data").type("VARCHAR").jdbcType(Types.VARCHAR).length(255).create())
                .setPrimaryKeyNames("id")
                .create();
    }

    /**
     * Test-only subclass that exposes the package-protected {@code tables()} accessor
     * so the test can pre-populate fake in-memory tables without performing real DB
     * schema-discovery queries.
     */
    private static class TestPostgresSchema extends PostgresSchema {

        TestPostgresSchema(PostgresConnectorConfig config, PostgresConnection connection) {
            super(config,
                    connection.getDefaultValueConverter(),
                    config.getTopicNamingStrategy(PostgresConnectorConfig.TOPIC_NAMING_STRATEGY),
                    TestHelper.getPostgresValueConverter(connection.getTypeRegistry(), config));
        }

        void addFakeTable(Table table) {
            tables().overwriteTable(table);
        }
    }
}
