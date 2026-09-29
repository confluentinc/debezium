/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.time.Duration;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.ThreadNameContext;
import io.debezium.util.Threads;

/**
 * Runs on task-0 (the leader) on a background thread: prepares the shared snapshot (slot create / export
 * + lock), publishes it on the coordination topic, then waits until every task has imported it -- or one
 * signals a restart -- before releasing the held connections.
 */
public class SmartSnapshotLeader implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmartSnapshotLeader.class);

    private final SmartSnapshotLifecycleManager lifecycle;
    private final Configuration config;
    private final CommonConnectorConfig connectorConfig;
    // The connector class, used only to name the leader thread following the standard Debezium thread-naming convention.
    private final Class<? extends SourceConnector> connectorType;
    // The leader owns its coordination facade end to end: it is created, started, used and stopped on the leader
    // thread inside run(). Nothing else holds it, so no other thread can use or close its Kafka client, and
    // SourceTask.start() never blocks on starting it. Set at the start of run() and only used on the leader thread.
    private SnapshotCoordinationFacade coordination;
    private final ErrorHandler errorHandler;
    private final int epoch;
    private final int numTasks;
    private final boolean shouldStream;
    private final long pollMs;
    // Bounds the wait for every task to join BEFORE the snapshot is prepared (no locks held yet).
    private final long joinWaitTimeoutMs;
    // Bounds the wait for every task to start its transaction AFTER the snapshot is prepared (locks held).
    private final long startedTransactionTimeoutMs;
    private final Runnable loggingContextSetup;
    // The thread run() executes on. Created by start(), read by stop() on the task-stop thread.
    private volatile Thread thread;

    public SmartSnapshotLeader(SmartSnapshotLifecycleManager lifecycle, ErrorHandler errorHandler, int epoch, int numTasks,
                               boolean shouldStream, Configuration config, CommonConnectorConfig connectorConfig,
                               Class<? extends SourceConnector> connectorType, Runnable loggingContextSetup) {
        this(lifecycle, errorHandler, epoch, numTasks, shouldStream, config, connectorConfig, connectorType,
                connectorConfig.getSmartSnapshotLeaderPollIntervalMs(),
                connectorConfig.getSmartSnapshotLeaderJoinWaitTimeoutMs(),
                connectorConfig.getSmartSnapshotLeaderStartedTransactionTimeoutMs(),
                loggingContextSetup);
    }

    // Visible for testing: set the timings directly.
    SmartSnapshotLeader(SmartSnapshotLifecycleManager lifecycle, ErrorHandler errorHandler, int epoch, int numTasks,
                        boolean shouldStream, Configuration config, CommonConnectorConfig connectorConfig,
                        Class<? extends SourceConnector> connectorType, long pollMs,
                        long joinWaitTimeoutMs, long startedTransactionTimeoutMs, Runnable loggingContextSetup) {
        this.lifecycle = lifecycle;
        this.config = config;
        this.connectorConfig = connectorConfig;
        this.connectorType = connectorType;
        this.errorHandler = errorHandler;
        this.epoch = epoch;
        this.numTasks = numTasks;
        this.shouldStream = shouldStream;
        this.pollMs = pollMs;
        this.joinWaitTimeoutMs = joinWaitTimeoutMs;
        this.startedTransactionTimeoutMs = startedTransactionTimeoutMs;
        this.loggingContextSetup = loggingContextSetup;
    }

    /**
     * Creates this leader's private coordination facade. Called once, on the leader thread, at the start of
     * {@link #run()}. Non-creating: tasks never create the coordination topic, the connector provisions it.
     * Visible for testing, so a test can hand in a mock instead of a Kafka-backed facade.
     */
    SnapshotCoordinationFacade createCoordination() {
        return SnapshotCoordinationFacade.nonCreating(config, connectorConfig);
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

            // Created here, on the leader thread, so creation and start run where the facade is used. A failure
            // (e.g. bad client config) lands in the catch below and fails the task after cleanup.
            coordination = createCoordination();
            // topic is provisioned by the connector before any task starts; fail fast if it is somehow missing
            coordination.start(SnapshotCoordination.MissingTopicPolicy.FAIL);

            // a completed task-0 that got restarted must NOT re-prepare, if other task can't finish the coordinator would start a new round
            if (coordination.isTaskDone("0", epoch)) {
                LOGGER.info("{} Snapshot already completed, skipping leader preparation", SmartSnapshotLogging.leader(epoch));
                // thread ends; no re-export, no re-lock, {server} key untouched. Foreground idles until downscale.
                return;
            }

            // Wait for every task to join BEFORE taking any locks. No snapshot is prepared yet, so this wait
            // costs the source database nothing; it just makes sure all tasks are up and already polling for
            // the snapshot info. That way, once we lock the tables below, the tasks attach almost at once and
            // the locked window (the critical section) stays as small as possible.
            //
            // The wait checks for a restart_needed marker on every poll, the first poll included, so a restart
            // that was already flagged for this epoch before the leader started (for example by a task that had
            // attached and then failed) is caught here too, before prepareSnapshot() creates the slot / exports
            // the snapshot / takes table locks for a round that is about to be thrown away.
            if (!waitForAllTasksJoined()) {
                // A restart was signaled, before or while waiting. Nothing is prepared and no locks are held, so
                // just end the thread; the monitor bumps the epoch and the round starts over. (A join timeout does
                // not land here — it throws, so the task is failed after this thread's cleanup.)
                return;
            }

            final SmartSnapshotLifecycleManager.SnapshotSetup setup = lifecycle.prepareSnapshot(shouldStream);

            // The whole table list travels in this one record, so the record size grows with the captured table
            // count: at ~120 bytes per serialized TableId it stays inside the 1 MiB default max.message.bytes up
            // to roughly 8k tables. Beyond that the write would be rejected and the round would fail; raising the
            // ceiling (topic-level compression, or splitting the assignments across records) is tracked
            // separately and is not needed for the table counts this supports today.
            // https://confluentinc.atlassian.net/browse/CC-43566
            coordination.writeSnapshotInfo(
                    setup.snapshotName(), setup.consistentPosition(),
                    setup.snapshotTxId(), epoch, setup.tables(),
                    numTasks);
            snapshotPublished = true;

            LOGGER.info("{} Prepared snapshot={}, LSN={}",
                    SmartSnapshotLogging.leader(epoch), setup.snapshotName(), setup.consistentPosition());

            // From here the table locks are held, so this wait is bounded: a task that joined but then died
            // before starting its transaction can no longer pin the locks forever.
            final SmartSnapshotPolling.Outcome startedTransactionOutcome = waitForAllTasksStartedTransaction();
            if (startedTransactionOutcome == SmartSnapshotPolling.Outcome.READY) {
                // hands the locks back where the connector needs it (MySQL: UNLOCK TABLES); the slot persists
                lifecycle.onAllTasksStartedTransaction();
                LOGGER.info("{} All tasks have started their transaction, stopping the leader thread", SmartSnapshotLogging.leader(epoch));
            }
            else if (startedTransactionOutcome == SmartSnapshotPolling.Outcome.ABORTED) {
                // A task already signaled a restart, so the monitor bumps the epoch; the finally below drops the locks.
                LOGGER.warn("{} Detected `restart_needed` marker, releasing locks early", SmartSnapshotLogging.leader(epoch));
            }
            else {
                // Timed out. The finally below drops the locks; make sure the round restarts too, so the monitor bumps
                // the epoch and retries from scratch. The restart signal is a single Kafka write, so the locks are held
                // for a negligible moment longer than before.
                LOGGER.warn("{} Timed out after {}ms waiting for all tasks to start their "
                        + "transaction, releasing locks and signaling restart", SmartSnapshotLogging.leader(epoch), startedTransactionTimeoutMs);
                signalRestart();
            }
        }
        catch (InterruptedException e) {
            // Path A: an interrupt-aware wait (the park inside a poll loop) was interrupted by the task-stop
            // path. The flag was cleared when InterruptedException was thrown, so restore it and end the thread.
            // The finally below releases whatever was held.
            LOGGER.info("{} Interrupted while waiting, stopping snapshot preparation", SmartSnapshotLogging.leader(epoch));
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
                LOGGER.error("{} Snapshot preparation aborted by shutdown, held connection closed", SmartSnapshotLogging.leader(epoch), throwable);
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
            // Clear the interrupt flag (and restore it at the end) instead of just reading it with isInterrupted():
            // the cleanup below must not run on an interrupted thread. coordination.stop() ends in
            // KafkaBasedLog.stop(), which join()s its work thread. With the flag set, that join() throws at once and
            // KafkaBasedLog gives up without closing its producer and consumer, leaking both. A task stop always
            // interrupts this thread, so this is the normal shutdown path, not a corner case.
            boolean wasInterrupted = Thread.interrupted();
            // The single release for every path out of the block above: success, restart signaled, join/started
            // timeout, interrupt, and failure. It is idempotent and a no-op when nothing is held, so the paths that
            // never prepared anything (already-done, restart signaled or timed out during the join wait) are
            // unaffected. It must run BEFORE the coordination facade is stopped: releasing drops the database locks,
            // which is the thing other tasks are waiting on, and it never touches Kafka. Guarded so a release failure
            // cannot skip the coordination shutdown below (or mask the failure being reported after this block).
            try {
                lifecycle.releaseSnapshot();
            }
            catch (Exception e) {
                LOGGER.warn("{} Failure while releasing the held snapshot connections. error={}", SmartSnapshotLogging.leader(epoch), e.getMessage());
            }
            try {
                // null only if creating the facade itself failed; then there is nothing to close
                if (coordination != null) {
                    LOGGER.info("{} Cleaning up snapshot coordination resources", SmartSnapshotLogging.leader(epoch));
                    // this is leader's private kafka based SnapshotCoordination
                    coordination.stop();
                }
            }
            catch (Exception e) {
                LOGGER.warn("{} Non-critical failure shutting down coordination log components. error={}", SmartSnapshotLogging.leader(epoch),
                        e.getMessage());
            }
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }

        // Signaled only now — after this thread's coordination facade is closed — so the task restart it triggers
        // does not race with (and interrupt) our own cleanup above.
        if (failure != null) {
            LOGGER.error("{} Snapshot preparation failed", SmartSnapshotLogging.leader(epoch), failure);
            errorHandler.setProducerThrowable(new DebeziumException(
                    SmartSnapshotLogging.leader(epoch) + " Snapshot preparation failed", failure));
        }
    }

    /**
     * Wait until every task has written its join marker, or until {@link #joinWaitTimeoutMs} elapses.
     * Returns true if all tasks joined, false if a restart was signaled for this epoch, either before the wait
     * started or while waiting (the leader must stop without preparing). Throws on timeout.
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
                SmartSnapshotLogging.leader(epoch), "all tasks to join", Duration.ofMillis(joinWaitTimeoutMs), Duration.ofMillis(pollMs),
                () -> {
                    if (coordination.anyRestartNeeded(numTasks, epoch)) {
                        LOGGER.warn("{} Detected `restart_needed` marker while waiting for tasks to join, "
                                + "skipping snapshot preparation", SmartSnapshotLogging.leader(epoch));
                        return SmartSnapshotPolling.PollResult.ABORT;
                    }
                    if (coordination.allTasksJoined(numTasks, epoch)) {
                        LOGGER.info("{} All {} tasks joined, preparing snapshot", SmartSnapshotLogging.leader(epoch), numTasks);
                        return SmartSnapshotPolling.PollResult.READY;
                    }
                    return SmartSnapshotPolling.PollResult.CONTINUE;
                },
                null);

        if (outcome == SmartSnapshotPolling.Outcome.TIMED_OUT) {
            LOGGER.warn("{} Timed out after {}ms waiting for all tasks to join; nothing prepared yet, "
                    + "failing the task to retry without bumping the epoch", SmartSnapshotLogging.leader(epoch), joinWaitTimeoutMs);
            throw new DebeziumException(
                    SmartSnapshotLogging.leader(epoch) + " Timed out waiting for all tasks to join");
        }
        return outcome == SmartSnapshotPolling.Outcome.READY;
    }

    /**
     * Wait until every task has started its snapshot transaction, or until {@link #startedTransactionTimeoutMs}
     * elapses. Table locks are held for the duration, so the timeout caps the critical section. Returns
     * {@link SmartSnapshotPolling.Outcome#READY} if all tasks started, {@link SmartSnapshotPolling.Outcome#ABORTED}
     * if a restart was signaled, or {@link SmartSnapshotPolling.Outcome#TIMED_OUT}, so the caller can tell the two
     * failure cases apart without reading the restart markers again. Throws {@link InterruptedException} when the
     * task is stopped. Calls keepAlive() each poll so the held connection/slot does not drop while waiting.
     */
    SmartSnapshotPolling.Outcome waitForAllTasksStartedTransaction() throws InterruptedException {
        // Table locks are held here, so a transient coordination read failure must NOT abort the round: that would
        // drop the locks and discard the prepared snapshot on a single broker blip. The shared poll loop logs and
        // keeps polling until the timeout, which is what caps the critical section.
        return SmartSnapshotPolling.pollUntil(
                SmartSnapshotLogging.leader(epoch), "all tasks to start their transaction",
                Duration.ofMillis(startedTransactionTimeoutMs), Duration.ofMillis(pollMs),
                () -> {
                    if (coordination.anyRestartNeeded(numTasks, epoch)) {
                        return SmartSnapshotPolling.PollResult.ABORT;
                    }
                    return coordination.allTasksStartedTransaction(numTasks, epoch)
                            ? SmartSnapshotPolling.PollResult.READY
                            : SmartSnapshotPolling.PollResult.CONTINUE;
                },
                // keep the held connections/slot alive while we wait, after each park
                lifecycle::keepAlive);
    }

    /**
     * Signal a restart of the round. restart_needed is keyed per task and the connector monitor scans every
     * task's marker, so writing it under task-0 (the leader) is enough to make the monitor bump the epoch and
     * reconfigure. Best-effort: if the write fails, task-0's rejoin path signals restart on its next start.
     */
    void signalRestart() {
        try {
            coordination.writeRestartNeeded("0", epoch);
        }
        catch (Exception e) {
            LOGGER.warn("{} Failed to write restart_needed; task-0 rejoin path will retry", SmartSnapshotLogging.leader(epoch), e);
        }
    }

    /**
     * Starts {@link #run()} on the leader's own background (daemon) thread. Called once by task-0.
     */
    public void start() {
        // Name the thread via the standard Debezium convention (honours connector.thread.name.pattern), the same way
        // ChangeEventSourceCoordinator and the connectors' other threads do. This includes the connector's logical
        // name, so two connectors' leader threads on the same worker/pod are distinguishable.
        final String threadName = Threads.buildThreadName(connectorType, connectorConfig.getLogicalName(),
                "smart-snapshot-leader", ThreadNameContext.from(connectorConfig));
        final Thread leaderThread = new Thread(this, threadName);
        leaderThread.setDaemon(true);
        this.thread = leaderThread;
        leaderThread.start();
    }

    /**
     * Stops the leader. This runs on the Kafka Connect task-stop thread, which is a different thread from the
     * leader thread. Safe to call if {@link #start()} was never called.
     * <p>
     * This does not touch any coordination facade: the leader closes its own in the {@code finally} of
     * {@link #run()}, and the task-side facade is closed by the task coordinator that uses it.
     * <p>
     * The steps must run in this order:
     * 1. interrupt() wakes the leader thread if it is parked in one of its poll loops.
     * 2. releaseSnapshot() closes the held connections. If the leader thread is waiting on a
     * database call that cannot be interrupted, closing the connection aborts that call so the
     * thread can finish. interrupt() is done first so that the error raised by the aborted
     * call is recognised as a shutdown rather than a real failure.
     * 3. join() waits for the leader thread to actually finish, which includes its own cleanup (releasing
     * the snapshot and closing its coordination facade), so a restarted task-0 does not start a new leader
     * while the old one is still tearing down. The wait is bounded so that stop can never block forever.
     */
    public void stop(long joinMs) {
        final Thread leaderThread = this.thread;

        // 1. Signal the leader thread to stop and unblock it wherever it may be waiting.
        // interrupt() wakes it from a park; releaseSnapshot() closes and aborts the held
        // connections, ending any query it is waiting on.
        if (leaderThread != null) {
            LOGGER.info("{} Stopping snapshot preparation and releasing held connections", SmartSnapshotLogging.leader(epoch));
            leaderThread.interrupt();
        }
        lifecycle.releaseSnapshot();

        // 2. Wait for the leader thread to finish its own cleanup. Bounded so stop can never block forever.
        if (leaderThread != null) {
            try {
                leaderThread.join(joinMs);
                if (leaderThread.isAlive()) {
                    LOGGER.warn("{} Leader thread did not stop within {} ms", SmartSnapshotLogging.leader(epoch), joinMs);
                }
            }
            catch (InterruptedException e) {
                LOGGER.warn("{} Task thread was interrupted while waiting for leader thread join", SmartSnapshotLogging.leader(epoch));
                Thread.currentThread().interrupt();
            }
        }
    }
}
