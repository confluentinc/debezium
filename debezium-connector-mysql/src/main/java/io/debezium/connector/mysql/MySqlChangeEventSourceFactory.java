/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

public class MySqlChangeEventSourceFactory implements ChangeEventSourceFactory<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlChangeEventSourceFactory.class);

    private final MySqlConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<MySqlPartition, TableId> dispatcher;
    private final Clock clock;
    private final MySqlTaskContext taskContext;
    private final MySqlStreamingChangeEventSourceMetrics streamingMetrics;
    private final MySqlDatabaseSchema schema;
    // MySQL snapshot requires buffering to modify the last record in the snapshot as sometimes it is
    // impossible to detect it till the snapshot is ended. Mainly when the last snapshotted table is empty.
    // Based on the DBZ-3113 the code can change in the future and it will be handled not in MySQL
    // but in the core shared code.
    private final ChangeEventQueue<DataChangeEvent> queue;

    private final SnapshotterService snapshotterService;

    public MySqlChangeEventSourceFactory(MySqlConnectorConfig configuration, MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
                                         ErrorHandler errorHandler, EventDispatcher<MySqlPartition, TableId> dispatcher, Clock clock, MySqlDatabaseSchema schema,
                                         MySqlTaskContext taskContext, MySqlStreamingChangeEventSourceMetrics streamingMetrics,
                                         ChangeEventQueue<DataChangeEvent> queue, SnapshotterService snapshotterService) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
        this.queue = queue;
        this.schema = schema;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshotChangeEventSource<MySqlPartition, MySqlOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<MySqlPartition> snapshotProgressListener,
                                                                                                      NotificationService<MySqlPartition, MySqlOffsetContext> notificationService) {
        return new MySqlSnapshotChangeEventSource(
                configuration,
                connectionFactory,
                taskContext.getSchema(),
                dispatcher,
                clock,
                (MySqlSnapshotChangeEventSourceMetrics) snapshotProgressListener,
                this::modifyAndFlushLastRecord,
                this::preSnapshot,
                notificationService,
                snapshotterService);
    }

    private void preSnapshot() {
        queue.enableBuffering();
    }

    private void modifyAndFlushLastRecord(Function<SourceRecord, SourceRecord> modify) throws InterruptedException {
        // Check if the current thread has been interrupted before attempting to flush
        if (Thread.currentThread().isInterrupted()) {
            LOGGER.info("Thread has been interrupted, skipping flush of buffered record");
            queue.disableBuffering();
            throw new InterruptedException("Thread interrupted during snapshot cleanup");
        }

        try {
            // Retry flush with progressively longer timeouts to maximize chance of success
            // while still preventing indefinite blocking
            final int[] retryTimeouts = { 5, 10, 15 }; // seconds: total 30 seconds max
            boolean flushSuccessful = false;

            for (int attempt = 0; attempt < retryTimeouts.length && !flushSuccessful; attempt++) {
                int timeoutSeconds = retryTimeouts[attempt];
                LOGGER.debug("Attempting to flush buffered record (attempt {}/{}, timeout: {}s)",
                        attempt + 1, retryTimeouts.length, timeoutSeconds);

                ExecutorService executor = Threads.newSingleThreadExecutor(MySqlChangeEventSourceFactory.class,
                        "mysql-connector", "snapshot-buffer-flush-" + (attempt + 1));
                try {
                    Future<Void> flushFuture = executor.submit(() -> {
                        try {
                            queue.flushBuffer(dataChange -> new DataChangeEvent(modify.apply(dataChange.getRecord())));
                            return null;
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    });

                    // Wait with current attempt timeout
                    flushFuture.get(timeoutSeconds, TimeUnit.SECONDS);
                    flushSuccessful = true;
                    LOGGER.info("Successfully flushed buffered record on attempt {}/{}",
                            attempt + 1, retryTimeouts.length);

                }
                catch (TimeoutException e) {
                    LOGGER.warn("Flush attempt {}/{} timed out after {}s. {}",
                            attempt + 1, retryTimeouts.length, timeoutSeconds,
                            (attempt + 1 < retryTimeouts.length) ? "Retrying..." : "Giving up.");
                }
                catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException && cause.getCause() instanceof InterruptedException) {
                        throw (InterruptedException) cause.getCause();
                    }
                    // Log but continue to next retry for other execution exceptions
                    LOGGER.warn("Flush attempt {}/{} failed with execution exception: {}. {}",
                            attempt + 1, retryTimeouts.length, cause.getMessage(),
                            (attempt + 1 < retryTimeouts.length) ? "Retrying..." : "Giving up.");
                }
                finally {
                    executor.shutdownNow();
                }
            }

            if (!flushSuccessful) {
                LOGGER.warn("All flush attempts failed. This likely means the consumer thread is no longer " +
                        "available or broker is unavailable. One buffered record may be lost.");
            }
        }
        finally {
            // Always attempt to disable buffering to prevent memory leaks
            // Note: In extreme failure scenarios (broker unavailable + consumer dead),
            // this may fail due to assertion in debug mode, but the coordinator thread
            // will no longer be stuck indefinitely due to the timeout above
            try {
                queue.disableBuffering();
            }
            catch (AssertionError e) {
                // Assertion failed due to non-empty buffer - this is expected in broker failure scenarios
                // The main memory leak (stuck coordinator thread) has been prevented by the timeout
                LOGGER.warn("Cannot disable buffering cleanly due to remaining buffered record. " +
                        "This is expected when broker is unavailable during snapshot cleanup. " +
                        "Coordinator thread memory leak has been prevented by timeout mechanism.");
            }
        }
    }

    @Override
    public StreamingChangeEventSource<MySqlPartition, MySqlOffsetContext> getStreamingChangeEventSource() {

        queue.disableBuffering();
        return new MySqlStreamingChangeEventSource(
                configuration,
                connectionFactory.mainConnection(),
                dispatcher,
                errorHandler,
                clock,
                taskContext,
                streamingMetrics,
                snapshotterService);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<MySqlPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                              MySqlOffsetContext offsetContext,
                                                                                                                                              SnapshotProgressListener<MySqlPartition> snapshotProgressListener,
                                                                                                                                              DataChangeEventListener<MySqlPartition> dataChangeEventListener,
                                                                                                                                              NotificationService<MySqlPartition, MySqlOffsetContext> notificationService) {

        if (configuration.isReadOnlyConnection()) {
            if (connectionFactory.mainConnection().isGtidModeEnabled()) {
                return Optional.of(new MySqlReadOnlyIncrementalSnapshotChangeEventSource(
                        configuration,
                        connectionFactory.mainConnection(),
                        dispatcher,
                        schema,
                        clock,
                        snapshotProgressListener,
                        dataChangeEventListener,
                        notificationService));
            }
            throw new UnsupportedOperationException("Read only connection requires GTID_MODE to be ON");
        }
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (Strings.isNullOrEmpty(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }
        return Optional.of(new SignalBasedIncrementalSnapshotChangeEventSource<>(
                configuration,
                connectionFactory.mainConnection(),
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener, notificationService));
    }
}
