/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.ErrorHandler;

/**
 * Runs on task-0 (the leader) on a background thread: prepares the shared snapshot (slot create / export
 * + lock), publishes it on the coordination topic, then waits until every task has imported it -- or one
 * signals a restart -- before releasing the held connections.
 */
public class SmartSnapshotLeader implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmartSnapshotLeader.class);

    private final SmartSnapshotLifecycleManager lifecycle;
    private final SnapshotCoordinationFacade leaderSnapshotCoordination;
    private final ErrorHandler errorHandler;
    private final int leaderEpoch;
    private final int numTasks;
    private final boolean shouldStream;
    private final long pollMs;
    // Bounds the wait for every task to join BEFORE the snapshot is prepared (no locks held yet).
    private final long joinWaitTimeoutMs;
    // Bounds the wait for every task to start its transaction AFTER the snapshot is prepared (locks held).
    private final long startedTransactionTimeoutMs;
    private final Runnable loggingContextSetup;

    public SmartSnapshotLeader(SmartSnapshotLifecycleManager lifecycle, SnapshotCoordinationFacade leaderSnapshotCoordination,
                               ErrorHandler errorHandler, int leaderEpoch, int numTasks, boolean shouldStream, long pollMs,
                               long joinWaitTimeoutMs, long startedTransactionTimeoutMs, Runnable loggingContextSetup) {
        this.lifecycle = lifecycle;
        this.leaderSnapshotCoordination = leaderSnapshotCoordination;
        this.errorHandler = errorHandler;
        this.leaderEpoch = leaderEpoch;
        this.numTasks = numTasks;
        this.shouldStream = shouldStream;
        this.pollMs = pollMs;
        this.joinWaitTimeoutMs = joinWaitTimeoutMs;
        this.startedTransactionTimeoutMs = startedTransactionTimeoutMs;
        this.loggingContextSetup = loggingContextSetup;
    }

    public SmartSnapshotLeader(SmartSnapshotLifecycleManager lifecycle, SnapshotCoordinationFacade leaderSnapshotCoordination,
                               ErrorHandler errorHandler, int leaderEpoch, int numTasks, boolean shouldStream, CommonConnectorConfig connectorConfig,
                               Runnable loggingContextSetup) {
        this(lifecycle, leaderSnapshotCoordination, errorHandler, leaderEpoch,
                numTasks, shouldStream, connectorConfig.getSmartSnapshotLeaderPollIntervalMs(),
                connectorConfig.getSmartSnapshotLeaderJoinWaitTimeoutMs(),
                connectorConfig.getSmartSnapshotLeaderStartedTransactionTimeoutMs(),
                loggingContextSetup);
    }

    @Override
    public void run() {
        // Tracks whether the snapshot has been published to the coordination topic. A failure AFTER this point
        // (e.g. keepAlive throwing because the DB connection was killed) means tasks may have attached, so the
        // round must be invalidated with a restart — unlike a failure before publishing, which has nothing to undo.
        boolean snapshotPublished = false;
        // The failure is signaled to the runtime only AFTER the finally has closed this thread's coordination
        // facade. setProducerThrowable triggers a task restart whose stop path interrupts this thread; doing it
        // before cleanup would interrupt our own Kafka client close mid-way and leave it half-closed.
        Throwable failure = null;
        try {
            loggingContextSetup.run();

            // topic is provisioned by the connector before any task starts; fail fast if it is somehow missing
            leaderSnapshotCoordination.start(SnapshotCoordination.MissingTopicPolicy.FAIL);

            // a completed task-0 that got restarted must NOT re-prepare, if other task can't finish the coordinator would start a new round
            if (leaderSnapshotCoordination.isTaskDone("0", leaderEpoch)) {
                LOGGER.info("Smart snapshot: [role=leader epoch={}] Snapshot already completed, skipping leader preparation", leaderEpoch);
                // thread ends; no re-export, no re-lock, {server} key untouched. Foreground idles until downscale.
                return;
            }

            // A restart was already flagged for this epoch (for example by a task that had attached and then
            // failed). The stop path will bounce this task anyway once the monitor acts on the marker, but
            // checking here is worth it: it skips prepareSnapshot(), so we do not create the slot / export the
            // snapshot / take table locks for a round that is about to be thrown away.
            if (anyRestartNeeded()) {
                LOGGER.info("Smart snapshot: [role=leader epoch={}] Detected restart_needed marker, skipping snapshot preparation", leaderEpoch);
                return;
            }

            // Wait for every task to join BEFORE taking any locks. No snapshot is prepared yet, so this wait
            // costs the source database nothing; it just makes sure all tasks are up and already polling for
            // the snapshot info. That way, once we lock the tables below, the tasks attach almost at once and
            // the locked window (the critical section) stays as small as possible.
            if (!waitForAllTasksJoined()) {
                // A restart was signaled while waiting. Nothing is prepared and no locks are held, so just end
                // the thread; the monitor bumps the epoch and the round starts over. (A join timeout does not
                // land here — it throws, so the task is failed after this thread's cleanup.)
                return;
            }

            final SmartSnapshotLifecycleManager.SnapshotSetup setup = lifecycle.prepareSnapshot(shouldStream);

            // The whole table list travels in this one record, so the record size grows with the captured table
            // count: at ~120 bytes per serialized TableId it stays inside the 1 MiB default max.message.bytes up
            // to roughly 8k tables. Beyond that the write would be rejected and the round would fail; raising the
            // ceiling (topic-level compression, or splitting the assignments across records) is tracked
            // separately and is not needed for the table counts this supports today.
            leaderSnapshotCoordination.writeSnapshotInfo(setup.snapshotName(), setup.consistentPosition(), leaderEpoch, setup.tables(), numTasks);
            snapshotPublished = true;

            LOGGER.info("Smart snapshot: [role=leader epoch={}] Prepared snapshot={}, LSN={}",
                    leaderEpoch, setup.snapshotName(), setup.consistentPosition());

            // From here the table locks are held, so this wait is bounded: a task that joined but then died
            // before starting its transaction can no longer pin the locks forever.
            if (waitForAllTasksStartedTransaction()) {
                // hands the locks back where the connector needs it (MySQL: UNLOCK TABLES); the slot persists
                lifecycle.onAllTasksStartedTransaction();
                LOGGER.info("Smart snapshot: [role=leader epoch={}] All tasks have started their transaction, stopping the leader thread", leaderEpoch);
            }
            else {
                // Timed out or a restart was signaled. The finally below drops the locks; make sure the round
                // restarts too, so the monitor bumps the epoch and retries from scratch. The restart signal is a
                // single Kafka write, so the locks are held for a negligible moment longer than before.
                if (anyRestartNeeded()) {
                    LOGGER.warn("Smart snapshot: [role=leader epoch={}] Detected `restart_needed` marker, releasing locks early", leaderEpoch);
                }
                else {
                    LOGGER.warn("Smart snapshot: [role=leader epoch={}] Timed out after {}ms waiting for all tasks to start their "
                            + "transaction, releasing locks and signaling restart", leaderEpoch, startedTransactionTimeoutMs);
                    signalRestart();
                }
            }
        }
        catch (InterruptedException e) {
            // Path A: an interrupt-aware wait (the park inside a poll loop) was interrupted by the task-stop
            // path. The flag was cleared when InterruptedException was thrown, so restore it and end the thread.
            // The finally below releases whatever was held.
            LOGGER.info("Smart snapshot: [role=leader epoch={}] Interrupted while waiting, stopping snapshot preparation", leaderEpoch);
            Thread.currentThread().interrupt();
        }
        catch (Throwable throwable) {
            // Catching Throwable ensures that an Error (for example NoClassDefFoundError or OutOfMemoryError)
            // also fails the task, instead of the thread dying silently and leaving the snapshot round stranded.
            // Every exit from this block releases the held snapshot (the finally below) and, for a real failure,
            // reports it through the errorHandler after cleanup — verified by SmartSnapshotLeaderTest
            // (onPreparationFailureReleasesAndFailsTheTask, keepAliveFailureAfterPublishReleasesSignalsRestartAndFailsAfterCleanup).
            if (Thread.currentThread().isInterrupted()) {
                // Path B: the thread was blocked in a call that ignores interrupts (a JDBC call), so
                // the task-stop path aborted it by closing the connection. The exception here is that
                // abort (for example a SQLException), not an InterruptedException, but the interrupt
                // flag is still set, which tells us this is a shutdown rather than a real failure.
                LOGGER.error("Smart snapshot: [role=leader epoch={}] Snapshot preparation aborted by shutdown, held connection closed", leaderEpoch, throwable);
                return;
            }
            // If the snapshot was already published (e.g. keepAlive threw because the DB connection was killed
            // during the started-transaction wait), tasks may have attached to it, so invalidate the round by
            // signaling a restart — the monitor then bumps the epoch. Otherwise a re-prepared leader could publish
            // a second snapshot at the SAME epoch while other tasks are still reading the first. A failure BEFORE
            // publishing has nothing to throw away, so we skip the signal (task-0's rejoin path covers a restart).
            if (snapshotPublished) {
                signalRestart();
            }
            // Defer failing the task until AFTER the finally has closed this thread's coordination facade, so the
            // restart it triggers cannot interrupt our own Kafka client close.
            failure = throwable;
        }
        finally {
            boolean wasInterrupted = Thread.interrupted();
            // The single release for every path out of the block above: success, restart signaled, join/started
            // timeout, interrupt, and failure. It is idempotent and a no-op when nothing is held, so the paths
            // that never prepared anything (already-done, restart-marker, join wait) are unaffected. It must run
            // BEFORE the coordination facade is stopped: releasing drops the database locks, which is the thing
            // other tasks are waiting on, and it never touches Kafka. Guarded so a release failure cannot skip
            // the coordination shutdown below (or mask the failure being reported after this block).
            try {
                lifecycle.releaseSnapshot();
            }
            catch (Exception e) {
                LOGGER.warn("Smart snapshot: [role=leader epoch={}] Failure while releasing the held snapshot connections. error={}", leaderEpoch, e.getMessage());
            }
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

        // Signaled only now — after this thread's coordination facade is closed — so the task restart it triggers
        // does not race with (and interrupt) our own cleanup above.
        if (failure != null) {
            LOGGER.error("Smart snapshot: [role=leader epoch={}] Snapshot preparation failed", leaderEpoch, failure);
            errorHandler.setProducerThrowable(new DebeziumException(
                    "Smart snapshot: [role=leader epoch=" + leaderEpoch + "] Snapshot preparation failed", failure));
        }
    }

    /**
     * Wait until every task has written its join marker, or until {@link #joinWaitTimeoutMs} elapses.
     * Returns true if all tasks joined, false if a restart was signaled while waiting (the leader must stop
     * without preparing). Throws on timeout.
     *
     * <p>On timeout we do NOT bump the epoch: nothing has been prepared, published, or locked yet, so there
     * is no partial work to throw away. We just fail the task; Kafka Connect restarts it and the join wait is
     * retried. Proactively bumping the epoch here would only churn rounds for no benefit.
     *
     * <p>The timeout throws rather than failing the task inline so that {@link #run()} reports it only after its
     * finally block has released the snapshot and closed this thread's coordination facade — the same deferral
     * the post-publish failure path uses, so the restart the failure triggers cannot interrupt our own cleanup.
     */
    boolean waitForAllTasksJoined() throws InterruptedException {
        final SmartSnapshotPolling.Outcome outcome = SmartSnapshotPolling.pollUntil(
                logPrefix(), "all tasks to join", Duration.ofMillis(joinWaitTimeoutMs), Duration.ofMillis(pollMs),
                () -> {
                    if (anyRestartNeeded()) {
                        LOGGER.warn("Smart snapshot: [role=leader epoch={}] Restart signaled while waiting for tasks to join, aborting round", leaderEpoch);
                        return SmartSnapshotPolling.PollResult.ABORT;
                    }
                    if (allTasksJoined()) {
                        LOGGER.info("Smart snapshot: [role=leader epoch={}] All {} tasks joined, preparing snapshot", leaderEpoch, numTasks);
                        return SmartSnapshotPolling.PollResult.READY;
                    }
                    return SmartSnapshotPolling.PollResult.CONTINUE;
                },
                null);

        if (outcome == SmartSnapshotPolling.Outcome.TIMED_OUT) {
            LOGGER.warn("Smart snapshot: [role=leader epoch={}] Timed out after {}ms waiting for all tasks to join; nothing prepared yet, "
                    + "failing the task to retry without bumping the epoch", leaderEpoch, joinWaitTimeoutMs);
            throw new DebeziumException(
                    "Smart snapshot: [role=leader epoch=" + leaderEpoch + "] Timed out waiting for all tasks to join");
        }
        return outcome == SmartSnapshotPolling.Outcome.READY;
    }

    /**
     * Wait until every task has started its snapshot transaction, or until {@link #startedTransactionTimeoutMs}
     * elapses. Table locks are held for the duration, so the timeout caps the critical section. Returns true if
     * all tasks started; false on timeout, restart signal, or interruption. Calls keepAlive() each poll so the
     * held connection/slot does not drop while waiting.
     */
    boolean waitForAllTasksStartedTransaction() throws InterruptedException {
        // Table locks are held here, so a transient coordination read failure must NOT abort the round: that would
        // drop the locks and discard the prepared snapshot on a single broker blip. The shared poll loop logs and
        // keeps polling until the timeout, which is what caps the critical section.
        final SmartSnapshotPolling.Outcome outcome = SmartSnapshotPolling.pollUntil(
                logPrefix(), "all tasks to start their transaction",
                Duration.ofMillis(startedTransactionTimeoutMs), Duration.ofMillis(pollMs),
                () -> {
                    if (anyRestartNeeded()) {
                        return SmartSnapshotPolling.PollResult.ABORT;
                    }
                    return allTasksStartedTransaction() ? SmartSnapshotPolling.PollResult.READY : SmartSnapshotPolling.PollResult.CONTINUE;
                },
                // keep the held connections/slot alive while we wait, after each park
                lifecycle::keepAlive);
        return outcome == SmartSnapshotPolling.Outcome.READY;
    }

    private String logPrefix() {
        return "Smart snapshot: [role=leader epoch=" + leaderEpoch + "]";
    }

    /**
     * Signal a restart of the round. restart_needed is keyed per task and the connector monitor scans every
     * task's marker, so writing it under task-0 (the leader) is enough to make the monitor bump the epoch and
     * reconfigure. Best-effort: if the write fails, task-0's rejoin path signals restart on its next start.
     */
    void signalRestart() {
        try {
            leaderSnapshotCoordination.writeRestartNeeded("0", leaderEpoch);
        }
        catch (Exception e) {
            LOGGER.warn("Smart snapshot: [role=leader epoch={}] Failed to write restart_needed; task-0 rejoin path will retry", leaderEpoch, e);
        }
    }

    boolean allTasksJoined() {
        for (int i = 0; i < numTasks; i++) {
            if (!leaderSnapshotCoordination.isTaskJoined(String.valueOf(i), leaderEpoch)) {
                return false;
            }
        }
        return true;
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
     * 1. interrupt() wakes the leader thread if it is sleeping in the keep-alive loop.
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
