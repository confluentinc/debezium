/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.pipeline.source.snapshot.SmartSnapshotHeldConnectionRegistry;

/**
 * Integration test for the one thing {@code HeldConnectionRegistryTest} cannot exercise with mocks: closing the
 * registry must abort a query that is genuinely in flight on a held connection, so a task stop can unblock a
 * leader prep thread that is parked in a non-interruptible JDBC call.
 */
public class SmartSnapshotHeldConnectionRegistryIT {

    @Test
    public void closeAbortsAnInFlightQueryOnAHeldConnection() throws Exception {
        final SmartSnapshotHeldConnectionRegistry registry = new SmartSnapshotHeldConnectionRegistry();
        final PostgresConnection connection = TestHelper.create();
        connection.setAutoCommit(false);
        registry.registerConnection("connection", connection);

        final CountDownLatch queryStarted = new CountDownLatch(1);
        final AtomicReference<Throwable> queryError = new AtomicReference<>();
        final AtomicLong unblockedAtNanos = new AtomicLong();
        final long startNanos = System.nanoTime();

        // A long server-side sleep stands in for the leader's long-held snapshot/lock query.
        final Thread queryThread = new Thread(() -> {
            try {
                queryStarted.countDown();
                connection.executeWithoutCommitting("SELECT pg_sleep(60)");
            }
            catch (Throwable t) {
                queryError.set(t);
            }
            finally {
                unblockedAtNanos.set(System.nanoTime());
            }
        }, "held-query");
        queryThread.start();

        // Make sure the query is actually executing on the server before we close.
        assertThat(queryStarted.await(5, SECONDS)).isTrue();
        Thread.sleep(1000);

        // The stop-thread action: closing the registry must abort the in-flight query (graceful close times out
        // after WAIT_FOR_CLOSE_SECONDS=10s, then conn.abort() fires).
        registry.close();

        queryThread.join(SECONDS.toMillis(45));
        assertThat(queryThread.isAlive()).as("held query thread should have been unblocked by close()").isFalse();
        assertThat(queryError.get()).as("the in-flight query should have failed, not completed normally").isNotNull();

        long elapsedSeconds = NANOSECONDS.toSeconds(unblockedAtNanos.get() - startNanos);
        // Prove close aborted the query rather than letting the 60s sleep run to completion.
        assertThat(elapsedSeconds).as("close() should abort well before the 60s sleep finishes").isLessThan(45);
    }
}
