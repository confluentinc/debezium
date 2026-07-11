/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * Runs on task-0 (the leader) on a background thread: prepares the shared snapshot (slot create / export
 * + lock), publishes it on the coordination topic, then waits until every task has imported it -- or one
 * signals a restart -- before releasing the held connections.
 */
public class SmartSnapshotLeader implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmartSnapshotLeader.class);
    private static final int POLL_MS = 10_000;

    private final SmartSnapshotLifecycleManager lifecycle;
    private final SnapshotCoordinationFacade leaderSnapshotCoordination;
    private final ErrorHandler errorHandler;
    private final int leaderEpoch;
    private final int numTasks;
    private final boolean shouldStream;
    private final long pollMs;
    private final Runnable loggingContextSetup;

    public SmartSnapshotLeader(SmartSnapshotLifecycleManager lifecycle, SnapshotCoordinationFacade leaderSnapshotCoordination,
                               ErrorHandler errorHandler, int leaderEpoch, int numTasks, boolean shouldStream, long pollMs,
                               Runnable loggingContextSetup) {
        this.lifecycle = lifecycle;
        this.leaderSnapshotCoordination = leaderSnapshotCoordination;
        this.errorHandler = errorHandler;
        this.leaderEpoch = leaderEpoch;
        this.numTasks = numTasks;
        this.shouldStream = shouldStream;
        this.pollMs = pollMs;
        this.loggingContextSetup = loggingContextSetup;
    }

    public SmartSnapshotLeader(SmartSnapshotLifecycleManager lifecycle, SnapshotCoordinationFacade leaderSnapshotCoordination,
                               ErrorHandler errorHandler, int leaderEpoch, int numTasks, boolean shouldStream,
                               Runnable loggingContextSetup) {
        this(lifecycle, leaderSnapshotCoordination, errorHandler, leaderEpoch, numTasks, shouldStream, POLL_MS, loggingContextSetup);
    }

    @Override
    public void run() {
        try {
            loggingContextSetup.run();

            // background thread — safe to block on the topic read here
            leaderSnapshotCoordination.start();

            // a completed task-0 that got restarted must NOT re-prepare, if other task can't finish the coordinator would start a new round
            if (leaderSnapshotCoordination.isTaskDone("0", leaderEpoch)) {
                LOGGER.info("Smart snapshot: [role=leader epoch={}] Snapshot already completed, skipping leader preparation", leaderEpoch);
                // thread ends; no re-export, no re-lock, {server} key untouched. Foreground idles until downscale.
                return;
            }

            // mid-prep restart already flagged
            if (anyRestartNeeded()) {
                LOGGER.info("Smart snapshot: [role=leader epoch={}] Detected restart_needed marker, skipping snapshot preparation", leaderEpoch);
                return;
            }

            final SmartSnapshotLifecycleManager.SnapshotSetup setup = lifecycle.prepareSnapshot(shouldStream);

            // todo list of tables might require compression or enable compression on the coordination topic
            leaderSnapshotCoordination.writeSnapshotInfo(setup.snapshotName(), setup.consistentPosition(), leaderEpoch, setup.tables(), numTasks);

            LOGGER.info("Smart snapshot: [role=leader epoch={}] Prepared snapshot={}, LSN={}",
                    leaderEpoch, setup.snapshotName(), setup.consistentPosition());

            // wait until every task has imported the snapshot and (optionally) locked its subset, then release and end the thread
            while (!Thread.currentThread().isInterrupted() && !allTasksStartedTransaction() && !anyRestartNeeded()) {
                Thread.sleep(pollMs);
                lifecycle.keepAlive();
            }
            // todo what if the thread is interrupted the previous loop would break
            // if the thread is indeed interrupted the kafka read would anyways fail
            if (allTasksStartedTransaction()) {
                // releaseSnapshot(), slot persists; thread ends
                lifecycle.onAllTasksStartedTransaction();
            }
            else if (anyRestartNeeded()) {
                LOGGER.warn("Smart snapshot: [role=leader epoch={}] Detected `restart_needed` marker, releasing early", leaderEpoch);
                // early abort: the connector monitor bumps epoch + reconfigures
                lifecycle.releaseSnapshot();
            }
            LOGGER.info("Smart snapshot: [role=leader epoch={}] All tasks have started their transaction, stopping the leader thread", leaderEpoch);
        }
        catch (InterruptedException e) {
            // Path A: an interrupt-aware wait (Thread.sleep in the keep-alive loop) was interrupted
            // by the task-stop path. The flag was cleared when InterruptedException was thrown, so
            // restore it and end the thread.
            LOGGER.info("Smart snapshot: [role=leader epoch={}] Interrupted while waiting, stopping snapshot preparation", leaderEpoch);
            Thread.currentThread().interrupt();
            // todo verify the behaviour
            // todo should we release snapshot here?
        }
        catch (Throwable throwable) {
            // todo verify the behaviour
            // Catching Throwable ensures that an Error (for example NoClassDefFoundError or OutOfMemoryError)
            // also fails the task, instead of the
            // thread dying silently and leaving the snapshot round stranded.
            lifecycle.releaseSnapshot();
            if (Thread.currentThread().isInterrupted()) {
                // Path B: the thread was blocked in a call that ignores interrupts (a JDBC call), so
                // the task-stop path aborted it by closing the connection. The exception here is that
                // abort (for example a SQLException), not an InterruptedException, but the interrupt
                // flag is still set, which tells us this is a shutdown rather than a real failure.
                LOGGER.error("Smart snapshot: [role=leader epoch={}] Snapshot preparation aborted by shutdown, held connection closed", leaderEpoch, throwable);
                return;
            }
            LOGGER.error("Smart snapshot: [role=leader epoch={}] Snapshot preparation failed", leaderEpoch, throwable);
            // Fail the task with the real error. We do NOT write restart_needed here: preparation failed,
            // so the snapshot was never published and there is nothing to throw away. When task-0
            // restarts it sees its own marker and writes restart_needed then,
            // which cause connector to bump the epoch.
            errorHandler
                    .setProducerThrowable(new DebeziumException("Smart snapshot: [role=leader epoch=" + leaderEpoch + "] Snapshot preparation failed", throwable));
        }
        finally {
            boolean wasInterrupted = Thread.interrupted();
            try {
                LOGGER.info("Smart snapshot: [role=leader epoch={}] Cleaning up snapshot coordination resources", leaderEpoch);
                // this is leader's private kafka based SnapshotCoordination
                leaderSnapshotCoordination.stop();
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot: [role=leader epoch={}] Non-critical failure shutting down coordination log components. error={}", leaderEpoch,
                        e.getMessage());
            }
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    boolean allTasksStartedTransaction() {
        for (int i = 0; i < numTasks; i++) {
            if (!leaderSnapshotCoordination.isTaskStartedTransaction(String.valueOf(i), leaderEpoch)) {
                return false;
            }
        }
        return true;
    }

    boolean anyRestartNeeded() {
        for (int i = 0; i < numTasks; i++) {
            if (leaderSnapshotCoordination.isRestartNeeded(String.valueOf(i), leaderEpoch)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Stops the smart snapshot related stuff for this task. This runs on the Kafka Connect task-stop
     * thread, which is a different thread from the leader thread.
     * <p>
     * Only smart snapshot tasks set up any of these resources, so for every other task this method
     * returns early. Even among smart snapshot tasks, the prep thread and lifecycle manager exist
     * only on the leader (task-0); followers have just the coordination facade.
     * <p>
     * The steps must run in this order:
     * 1. interrupt() wakes the prep thread if it is sleeping in the keep-alive loop.
     * 2. releaseSnapshot() closes the held connections. If the prep thread is waiting on a
     * database call that cannot be interrupted, closing the connection aborts that call so the
     * thread can finish. interrupt() is done first so that the error raised by the aborted
     * call is recognised as a shutdown rather than a real failure.
     * 3. join() waits for the prep thread to actually finish, so that it is no longer using the
     * coordination facade when we stop it in the next step. The wait is bounded so that stop
     * can never block forever.
     * 4. stop() closes the coordination facade last, because it wraps a Kafka client that is not
     * safe to use on one thread and close on another at the same time.
     */
    public static void stopSmartSnapshot(Thread leaderThread, SmartSnapshotLifecycleManager lifecycle,
                                         SnapshotCoordinationFacade coordinationFacade, long joinMs, String taskId, int epoch) {
        // 1. Signal the leader thread to stop and unblock it wherever it may be waiting.
        // interrupt() wakes it from sleep(); releaseSnapshot() closes and aborts the held
        // connections, ending any query it is waiting on.
        if (leaderThread != null) {
            LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Stopping snapshot preparation and releasing held connections", taskId, epoch);
            leaderThread.interrupt();
        }
        if (lifecycle != null) {
            lifecycle.releaseSnapshot();
        }

        boolean currentThreadWasInterrupted = false;

        // 2. Wait for the prep thread to finish, so it is no longer using the coordination
        // facade when we close it below. Bounded so stop can never block forever.
        if (leaderThread != null) {
            try {
                leaderThread.join(joinMs);
                if (leaderThread.isAlive()) {
                    LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Leader thread did not stop within {} ms", taskId, epoch, joinMs);
                }
            }
            catch (InterruptedException e) {
                LOGGER.warn("Smart snapshot: [role=task taskId={} epoch={}] Task thread was interrupted while waiting for leader thread join", taskId, epoch);
                currentThreadWasInterrupted = true;
            }
        }
        if (coordinationFacade != null) {
            try {
                LOGGER.info("Smart snapshot: [role=task taskId={} epoch={}] Stopping coordination facade", taskId, epoch);
                coordinationFacade.stop();
            }
            catch (Exception e) {
                LOGGER.error("Smart snapshot: [role=task taskId={} epoch={}] Failed to cleanly close coordination facade log. error={}", taskId, epoch, e.getMessage());
            }
        }

        if (currentThreadWasInterrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
