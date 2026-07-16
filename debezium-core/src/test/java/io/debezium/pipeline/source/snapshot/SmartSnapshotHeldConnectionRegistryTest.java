/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;

public class SmartSnapshotHeldConnectionRegistryTest {

    @Test
    public void keepAlivePingsEveryRegisteredConnection() throws Exception {
        SmartSnapshotHeldConnectionRegistry registry = new SmartSnapshotHeldConnectionRegistry();
        JdbcConnection a = mock(JdbcConnection.class);
        JdbcConnection b = mock(JdbcConnection.class);
        registry.registerConnection("a", a);
        registry.registerConnection("b", b);

        registry.keepAlive();

        verify(a).executeWithoutCommitting("SELECT 1");
        verify(b).executeWithoutCommitting("SELECT 1");
    }

    @Test
    public void keepAliveThrowsWhenAConnectionIsDead() throws Exception {
        SmartSnapshotHeldConnectionRegistry registry = new SmartSnapshotHeldConnectionRegistry();
        JdbcConnection dead = mock(JdbcConnection.class);
        when(dead.executeWithoutCommitting("SELECT 1")).thenThrow(new SQLException("connection lost"));
        registry.registerConnection("dead", dead);

        assertThatThrownBy(registry::keepAlive).isInstanceOf(DebeziumException.class);
    }

    @Test
    public void keepAliveDoesNotPingResources() throws Exception {
        SmartSnapshotHeldConnectionRegistry registry = new SmartSnapshotHeldConnectionRegistry();
        // A JdbcConnection registered as a resource (close-only) must NOT be liveness-checked.
        JdbcConnection resource = mock(JdbcConnection.class);
        registry.registerResource("resource", resource);

        registry.keepAlive();

        verify(resource, never()).executeWithoutCommitting("SELECT 1");
    }

    @Test
    public void closeClosesConnectionsAndResources() throws Exception {
        SmartSnapshotHeldConnectionRegistry registry = new SmartSnapshotHeldConnectionRegistry();
        JdbcConnection connection = mock(JdbcConnection.class);
        AutoCloseable resource = mock(AutoCloseable.class);
        registry.registerConnection("connection", connection);
        registry.registerResource("resource", resource);

        registry.close();

        verify(connection).close();
        verify(resource).close();
    }

    @Test
    public void closeIsIdempotent() throws Exception {
        SmartSnapshotHeldConnectionRegistry registry = new SmartSnapshotHeldConnectionRegistry();
        JdbcConnection connection = mock(JdbcConnection.class);
        registry.registerConnection("connection", connection);

        registry.close();
        registry.close();

        verify(connection, times(1)).close();
    }

    @Test
    public void closeSwallowsCloseFailures() throws Exception {
        SmartSnapshotHeldConnectionRegistry registry = new SmartSnapshotHeldConnectionRegistry();
        JdbcConnection failing = mock(JdbcConnection.class);
        doThrow(new SQLException("boom")).when(failing).close();
        AutoCloseable other = mock(AutoCloseable.class);
        registry.registerConnection("failing", failing);
        registry.registerResource("other", other);

        // A failure closing one resource must not stop the others from being closed.
        registry.close();

        verify(other).close();
    }

    @Test
    public void registerAfterCloseClosesTheResourceAndThrows() throws Exception {
        SmartSnapshotHeldConnectionRegistry registry = new SmartSnapshotHeldConnectionRegistry();
        registry.close();

        JdbcConnection late = mock(JdbcConnection.class);
        assertThatThrownBy(() -> registry.registerConnection("late", late)).isInstanceOf(DebeziumException.class);
        // the connection opened during a racing prepare is closed immediately so it does not leak
        verify(late).close();
        // and it is not tracked, so a later keepAlive does not touch it
        registry.keepAlive();
        verify(late, never()).executeWithoutCommitting("SELECT 1");
    }

    @Test
    public void keepAliveOnEmptyRegistryIsNoOp() {
        SmartSnapshotHeldConnectionRegistry registry = new SmartSnapshotHeldConnectionRegistry();
        // nothing registered → no exception
        registry.keepAlive();
        assertThat(registry).isNotNull();
    }

    /**
     * The registry exists to be safe when the leader prep thread registers a connection while the task-stop
     * thread closes concurrently. Whichever thread wins the lock, the connection must be closed exactly once —
     * never leaked (registered-but-never-closed) and never double-closed. Run many overlapping rounds to shake
     * out interleavings; the post-condition holds for every interleaving, so the assertion is not flaky.
     */
    @Test
    public void concurrentRegisterAndCloseClosesTheConnectionExactlyOnce() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 500; i++) {
                final SmartSnapshotHeldConnectionRegistry registry = new SmartSnapshotHeldConnectionRegistry();
                final JdbcConnection connection = mock(JdbcConnection.class);
                final CyclicBarrier startTogether = new CyclicBarrier(2);

                Future<?> registering = pool.submit(() -> {
                    awaitBarrier(startTogether);
                    try {
                        registry.registerConnection("connection", connection);
                    }
                    catch (DebeziumException abortedBecauseClosedFirst) {
                        // expected when close() won the race: register aborts and closes the connection itself
                    }
                });
                Future<?> closing = pool.submit(() -> {
                    awaitBarrier(startTogether);
                    registry.close();
                });

                registering.get(5, TimeUnit.SECONDS);
                closing.get(5, TimeUnit.SECONDS);

                // no leak and no double close, regardless of who won the lock
                verify(connection, times(1)).close();
            }
        }
        finally {
            pool.shutdownNow();
        }
    }

    private static void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await(5, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            throw new IllegalStateException("barrier await failed", e);
        }
    }
}
