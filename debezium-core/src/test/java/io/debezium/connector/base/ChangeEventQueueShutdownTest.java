/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import io.debezium.pipeline.DataChangeEvent;
import io.debezium.util.LoggingContext;

/**
 * Test for the queue shutdown mechanism that prevents coordinator thread memory leaks.
 *
 * This test validates the fix for the issue where coordinator threads get stuck
 * in infinite blocking loops when the queue is full and the consumer thread dies.
 *
 * @author Yashi Srivastava
 */
public class ChangeEventQueueShutdownTest {

    /**
     * Test that shutdown mechanism prevents infinite blocking in doEnqueue
     */
    @Test
    public void shouldUnblockThreadsOnShutdown() throws Exception {
        int queueSize = 5;
        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(3)
                .maxQueueSize(queueSize)
                .loggingContextSupplier(() -> LoggingContext.forConnector("test", "test", "test"))
                .pollInterval(Duration.ofMillis(100))
                .build();

        DataChangeEvent testEvent = new DataChangeEvent(null);
        for (int i = 0; i < queueSize; i++) {
            queue.doEnqueue(testEvent);
        }

        AtomicBoolean enqueueCompleted = new AtomicBoolean(false);
        AtomicReference<Exception> enqueueException = new AtomicReference<>();
        CountDownLatch enqueueStarted = new CountDownLatch(1);
        CountDownLatch enqueueFinished = new CountDownLatch(1);

        Thread enqueueThread = new Thread(() -> {
            try {
                enqueueStarted.countDown();
                // This should block because queue is full
                queue.doEnqueue(testEvent);
                enqueueCompleted.set(true);
            }
            catch (Exception e) {
                enqueueException.set(e);
            }
            finally {
                enqueueFinished.countDown();
            }
        });

        enqueueThread.start();

        // Wait for thread to start and get blocked
        assertTrue("Enqueue thread should start", enqueueStarted.await(5, TimeUnit.SECONDS));
        Thread.sleep(200); // Give time to reach blocking condition

        // Verify thread is alive and blocked
        assertTrue("Thread should be alive and blocked", enqueueThread.isAlive());

        // Shutdown queue - should unblock the thread
        queue.shutdown();

        // Wait for thread to finish
        boolean threadFinished = enqueueFinished.await(5, TimeUnit.SECONDS);

        // Verify shutdown worked
        assertTrue("Thread should finish after shutdown", threadFinished);

        // Verify the enqueue was interrupted
        assertFalse("Enqueue should not complete after shutdown", enqueueCompleted.get());
        assertTrue("Should have an exception", enqueueException.get() != null);
        assertTrue("Should be InterruptedException", enqueueException.get() instanceof InterruptedException);
        assertTrue("Should mention shutdown", enqueueException.get().getMessage().contains("shut down"));
    }

    /**
     * Test that shutdown is idempotent
     */
    @Test
    public void shouldHandleMultipleShutdownCalls() throws Exception {
        int queueSize = 5;
        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(5)
                .maxQueueSize(queueSize)
                .loggingContextSupplier(() -> LoggingContext.forConnector("test", "test", "test"))
                .pollInterval(Duration.ofMillis(100))
                .build();

        // Fill the queue to capacity first
        DataChangeEvent testEvent = new DataChangeEvent(null);
        for (int i = 0; i < queueSize; i++) {
            queue.doEnqueue(testEvent);
        }

        // Multiple shutdown calls should not cause issues
        queue.shutdown();
        queue.shutdown();
        queue.shutdown();

        // Enqueue on shutdown queue should fail when it hits the blocking condition
        try {
            queue.doEnqueue(testEvent);
            assertTrue("Enqueue should fail on shutdown queue", false);
        }
        catch (InterruptedException e) {
            assertTrue("Should mention shutdown", e.getMessage().contains("shut down"));
        }
    }
}