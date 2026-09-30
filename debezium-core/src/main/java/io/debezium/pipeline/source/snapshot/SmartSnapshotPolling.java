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
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Threads;

/**
 * The one poll loop used by every smart snapshot wait: the leader waiting for tasks to join, the leader
 * waiting for tasks to start their transaction, and a task waiting for the leader to publish the snapshot
 * info. All three are "poll the coordination topic until a condition holds, until something makes waiting
 * pointless, or until a timeout elapses", and all three must behave the same way about interrupts, transient
 * read failures and log volume — so the skeleton lives here instead of being re-implemented three times.
 *
 * <p>Transient failures: a {@link DebeziumException} thrown by the poll body (a coordination read that hit a
 * broker blip) is treated as "not ready yet" — it is logged and the loop keeps polling. The retry is bounded by
 * the caller's timeout rather than by a separate attempt count: the caller that holds table locks must not
 * abort the round early on a read blip, and the ones that hold nothing gain nothing by failing before their
 * deadline. Because the retry lives here, callers do not re-implement it.
 *
 * <p>Interrupts: the loop parks between polls with a {@link Metronome}, so an interrupt from the task-stop path
 * surfaces as {@link InterruptedException} and is propagated to the caller, which is what ends the wait.
 */
final class SmartSnapshotPolling {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmartSnapshotPolling.class);

    /**
     * Log the "still waiting" line at INFO on the first attempt and then once every this many attempts.
     */
    private static final int INFO_LOG_EVERY_ATTEMPTS = 6;

    /** What a single poll of the condition concluded. */
    enum PollResult {
        /** The condition is satisfied; stop polling. */
        READY,
        /** Something makes waiting pointless (a restart was signaled, a newer epoch appeared); stop polling. */
        ABORT,
        /** Not yet; keep polling. */
        CONTINUE
    }

    /** How the wait ended. */
    enum Outcome {
        READY,
        ABORTED,
        TIMED_OUT
    }

    @FunctionalInterface
    interface Poll {
        PollResult poll();
    }

    private SmartSnapshotPolling() {
    }

    /**
     * Poll {@code condition} every {@code interval} until it reports {@link PollResult#READY} or
     * {@link PollResult#ABORT}, or until {@code timeout} elapses.
     *
     * @param logPrefix     the caller's role/epoch prefix, prepended to every log line emitted here
     * @param reason          what is being waited for, used in the log lines ("all tasks to join", ...)
     * @param afterEachPoll optional action run after each park (the leader's keep-alive of the held
     *                      connections); may be {@code null}
     * @return how the wait ended; the caller decides what a timeout or an abort means
     */
    static Outcome pollUntil(String logPrefix, String reason, Duration timeout, Duration interval,
                             Poll condition, Runnable afterEachPoll)
            throws InterruptedException {
        final Threads.Timer timer = Threads.timer(Clock.SYSTEM, timeout);
        final Metronome metronome = Metronome.parker(interval, Clock.SYSTEM);
        int attempt = 0;
        while (!timer.expired()) {
            try {
                final PollResult result = condition.poll();
                if (result == PollResult.READY) {
                    return Outcome.READY;
                }
                if (result == PollResult.ABORT) {
                    return Outcome.ABORTED;
                }
            }
            catch (DebeziumException e) {
                LOGGER.warn("{} Transient coordination read failure while waiting for {} (attempt {}), retrying", logPrefix, reason, attempt + 1, e);
            }

            attempt++;
            // Throttled: a long wait across many tasks would otherwise flood the log at INFO.
            if (attempt == 1 || attempt % INFO_LOG_EVERY_ATTEMPTS == 0) {
                LOGGER.info("{} Waiting for {} (attempt {}, timeout {}ms)", logPrefix, reason, attempt, timeout.toMillis());
            }
            else {
                LOGGER.debug("{} Waiting for {} (attempt {}, timeout {}ms)", logPrefix, reason, attempt, timeout.toMillis());
            }
            metronome.pause();
            if (afterEachPoll != null) {
                afterEachPoll.run();
            }
        }
        return Outcome.TIMED_OUT;
    }
}
