/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;

/**
 * Integration test that reproduces the coordinator thread leak when
 * {@link PostgresSchema#refresh(PostgresConnection, boolean)} runs over a large
 * number of tables and a rebalance triggers shutdown mid-way.
 *
 * <p>Strategy: Create {@value #TABLE_COUNT} real tables. Invoke the production
 * {@code PostgresSchema.refresh()} on a worker thread. After 2s, trigger the
 * coordinator's two-phase shutdown (4s graceful + 4s forced interrupt).
 *
 * <p>Without the fix, the per-table loop in {@code refresh()} has no interrupt
 * checks, so the worker thread runs to completion (~20s) and the test fails.
 * With the fix, periodic interrupt checks (every 1% of tables) abort the loop
 * within seconds of the interrupt being delivered.
 *
 * @author Yashi Srivastava
 */
public class CoordinatorThreadLeakIT extends AbstractRecordsProducerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorThreadLeakIT.class);

    /**
     * Number of tables to create. Sized so that {@code refresh()} runs long
     * enough (~15-25s) to be interrupted mid-loop, but small enough that
     * setup/teardown remain practical for an integration test.
     */
    private static final int TABLE_COUNT = 1000;

    private PostgresConnection connection;
    private PostgresSchema schema;
    private PostgresConnectorConfig config;

    @Before
    public void before() throws Exception {
        TestHelper.dropAllSchemas();
        initializeConnectorTestFramework();
        TestHelper.dropDefaultReplicationSlot();

        LOGGER.info("📦 Creating {} test tables (this may take a moment)...", TABLE_COUNT);
        long createStart = System.currentTimeMillis();

        StringBuilder ddl = new StringBuilder();
        ddl.append("DROP SCHEMA IF EXISTS test CASCADE;");
        ddl.append("CREATE SCHEMA test;");
        for (int i = 1; i <= TABLE_COUNT; i++) {
            ddl.append(String.format("CREATE TABLE test.table_%05d (id SERIAL PRIMARY KEY, data TEXT);", i));
            ddl.append(String.format("ALTER TABLE test.table_%05d REPLICA IDENTITY FULL;", i));
        }
        TestHelper.execute(ddl.toString());

        LOGGER.info("📦 Created {} tables in {} ms", TABLE_COUNT, System.currentTimeMillis() - createStart);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "test")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "test\\..*");

        config = new PostgresConnectorConfig(configBuilder.build());
        connection = TestHelper.createWithTypeRegistry();
        schema = TestHelper.getSchema(config, connection.getTypeRegistry());
    }

    @After
    public void after() throws Exception {
        if (connection != null) {
            connection.close();
        }
        TestHelper.execute("DROP SCHEMA IF EXISTS test CASCADE;");
    }

    /**
     * Reproduces the thread leak: send interrupt() while the production
     * {@link PostgresSchema#refresh(PostgresConnection, boolean)} is iterating
     * over {@value #TABLE_COUNT} tables, and check if the worker thread responds.
     *
     * <p>NOTE: This test depends on a temporary 50ms-per-table delay added to
     * {@code PostgresSchema.refresh()} (see source comments). With 1000 tables that
     * makes refresh take ~50s, which is long enough to interrupt mid-loop.
     *
     * <p>Without fix → worker ignores interrupt, keeps running to completion → FAIL.
     * <p>With fix    → worker exits at next 1%-of-tables interrupt-check → PASS.
     */
    @Test
    public void testCoordinatorThreadLeakDuringSchemaLoad() throws Exception {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        CountDownLatch schemaLoadStarted = new CountDownLatch(1);

        Thread coordinatorThread = new Thread(() -> {
            try {
                LOGGER.info("🎯 Coordinator thread started — invoking production PostgresSchema.refresh() over {} tables",
                        TABLE_COUNT);
                schemaLoadStarted.countDown();

                // Production code under test — same call path as
                // PostgresStreamingChangeEventSource.initSchema() → taskContext.refreshSchema(connection, true)
                schema.refresh(connection, true);

                LOGGER.info("🎯 Coordinator thread completed schema refresh normally");
            }
            catch (SQLException e) {
                if (Thread.currentThread().isInterrupted() || e.getMessage().contains("interrupted")) {
                    LOGGER.info("🎯 Coordinator thread interrupted: {}", e.getMessage());
                }
                else {
                    LOGGER.error("🎯 Coordinator thread failed with unexpected error", e);
                }
            }
            finally {
                LOGGER.info("🎯 Coordinator thread exiting");
            }
        }, "test-change-event-source-coordinator");

        try {
            coordinatorThread.start();

            assertThat(schemaLoadStarted.await(5, TimeUnit.SECONDS))
                    .as("Schema load should start within 5 seconds")
                    .isTrue();

            // Let refresh() get well into the per-table loop
            Thread.sleep(2000);
            LOGGER.info("⏰ Schema load has been running for 2 seconds");

            ThreadInfo threadInfo = threadMXBean.getThreadInfo(coordinatorThread.getId());
            LOGGER.info("📊 Coordinator thread state before interrupt: {}", threadInfo.getThreadState());

            // Send interrupt — exactly what executor.shutdownNow() does
            LOGGER.info("🔄 Sending interrupt() to coordinator thread");
            long interruptTime = System.currentTimeMillis();
            coordinatorThread.interrupt();

            // Give the thread a generous window to respond. If interrupt checks exist in
            // the per-table loop, the thread should exit within a few iterations.
            // Without the fix, the thread will run to completion (~50s for 1000 tables).
            final long responseDeadlineMs = 5000;
            LOGGER.info("⏳ Waiting up to {}ms for coordinator thread to respond to interrupt...", responseDeadlineMs);
            coordinatorThread.join(responseDeadlineMs);

            long responseTime = System.currentTimeMillis() - interruptTime;
            boolean stillAlive = coordinatorThread.isAlive();

            if (stillAlive) {
                threadInfo = threadMXBean.getThreadInfo(coordinatorThread.getId());
                if (threadInfo != null) {
                    LOGGER.error("📊 Coordinator thread still alive at {}ms after interrupt", responseTime);
                    LOGGER.error("📊 Stack trace:");
                    Arrays.stream(threadInfo.getStackTrace()).limit(15).forEach(st -> LOGGER.error("  at {}", st));
                }
            }
            else {
                LOGGER.info("✅ Coordinator thread responded to interrupt in {}ms", responseTime);
            }

            // Drain the thread before asserting (so JUnit teardown doesn't hang)
            LOGGER.info("⏳ Draining thread (max 120s) for clean teardown...");
            coordinatorThread.join(120_000);

            if (stillAlive) {
                fail(String.format(
                        "❌ THREAD LEAK BUG CONFIRMED: Coordinator thread did not respond to interrupt within %dms.%n" +
                                "  In production this becomes a zombie thread because ChangeEventSourceCoordinator.stop()%n" +
                                "  gives up after 8 seconds (4s graceful + 4s forced) and returns.%n" +
                                "  Apply the fix (interrupt checks in PostgresSchema.refresh per-table loop) and this test should pass.",
                        responseDeadlineMs));
            }

            LOGGER.info("✅ TEST PASSED: Coordinator thread responded to interrupt within {}ms", responseDeadlineMs);
        }
        finally {
            if (coordinatorThread.isAlive()) {
                LOGGER.warn("🧹 Cleanup: thread still alive, interrupting again");
                coordinatorThread.interrupt();
                coordinatorThread.join(5000);
            }
        }
    }
}
