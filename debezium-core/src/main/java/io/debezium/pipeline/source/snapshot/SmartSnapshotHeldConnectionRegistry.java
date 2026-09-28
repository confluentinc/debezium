/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;

/**
 * Thread-safe registry of the connections a smart-snapshot leader holds open while it prepares the shared
 * snapshot (slot/export and lock connections). The leader thread registers connections as it opens them,
 * while the task-stop thread may close them concurrently.
 *
 * <p>Only the registry's own state is guarded. Connections are closed <em>outside</em> the lock so that closing
 * one can abort an in-flight query running on the leader thread, which is how a stop unblocks prepare that is
 * waiting on a non-interruptible JDBC call.
 *
 * <p>Each connection is registered with a name, and the registry can be given a log prefix (e.g. the
 * leader's {@code [role=leader epoch=N]} context), so close/liveness log messages still identify which connection
 * and which round they refer to.
 */
public class SmartSnapshotHeldConnectionRegistry implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmartSnapshotHeldConnectionRegistry.class);
    private static final String LIVENESS_QUERY = "SELECT 1";

    // Prefixed to every log/exception message so it carries the caller's context (role/epoch). Ends with a
    // trailing space when set, so it reads naturally in front of the message; empty when not provided.
    private final String logPrefix;
    private final ReentrantLock lock = new ReentrantLock();
    private final List<Held> held = new ArrayList<>();
    private boolean closed;

    public SmartSnapshotHeldConnectionRegistry() {
        this("");
    }

    public SmartSnapshotHeldConnectionRegistry(String logPrefix) {
        this.logPrefix = (logPrefix == null || logPrefix.isEmpty()) ? "" : logPrefix + " ";
    }

    /**
     * Track a connection, identified by {@code name}, that must be closed on {@link #close()} and liveness-checked
     * on {@link #keepAlive()}.
     */
    public void registerConnection(String name, JdbcConnection connection) {
        register(name, connection, connection);
    }

    /**
     * Track a resource, identified by {@code name}, that must be closed on {@link #close()} but is not
     * liveness-checked — e.g. a replication connection that cannot run the liveness query and has its own
     * liveness semantics.
     */
    public void registerResource(String name, AutoCloseable resource) {
        register(name, null, resource);
    }

    /**
     * If the registry is already closed, the resource is closed immediately and this throws, so a prepare that
     * races a stop aborts instead of leaking the connection or publishing during shutdown.
     */
    private void register(String name, JdbcConnection livenessConnection, AutoCloseable resource) {
        final boolean alreadyClosed;
        lock.lock();
        try {
            alreadyClosed = closed;
            if (!alreadyClosed) {
                held.add(new Held(name, livenessConnection, resource));
            }
        }
        finally {
            lock.unlock();
        }
        if (alreadyClosed) {
            closeQuietly(name, resource);
            throw new DebeziumException(logPrefix + name + " connection registered after the registry was closed; aborting");
        }
    }

    /**
     * Runs a liveness query on every registered connection, throwing if any is dead so the caller can fail fast
     * (the held snapshot/lock has been lost).
     */
    public void keepAlive() {
        final List<Held> current;
        lock.lock();
        try {
            current = new ArrayList<>(held);
        }
        finally {
            lock.unlock();
        }
        for (Held h : current) {
            if (h.livenessConnection == null) {
                continue;
            }
            try {
                // runs inside the held snapshot/lock transaction — harmless, resets the idle timer
                h.livenessConnection.executeWithoutCommitting(LIVENESS_QUERY);
            }
            catch (Exception e) {
                throw new DebeziumException(logPrefix + h.name + " connection is dead; the held snapshot/lock has been lost", e);
            }
        }
    }

    /**
     * Closes every registered connection and resource. Idempotent; after this, {@link #registerConnection} /
     * {@link #registerResource} abort.
     */
    @Override
    public void close() {
        final List<Held> toClose;
        lock.lock();
        try {
            closed = true;
            toClose = new ArrayList<>(held);
            held.clear();
        }
        finally {
            lock.unlock();
        }
        for (Held h : toClose) {
            closeQuietly(h.name, h.resource);
        }
    }

    private void closeQuietly(String name, AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        }
        catch (Exception e) {
            LOGGER.warn("{}Error closing {} connection", logPrefix, name, e);
        }
    }

    private static final class Held {
        private final String name;
        private final JdbcConnection livenessConnection; // null for close-only resources
        private final AutoCloseable resource;

        Held(String name, JdbcConnection livenessConnection, AutoCloseable resource) {
            this.name = name;
            this.livenessConnection = livenessConnection;
            this.resource = resource;
        }
    }
}
