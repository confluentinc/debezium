/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.debezium.DebeziumException;

/**
 * Unit tests for the shared smart snapshot poll loop. Every leader/task wait runs on this loop, so the
 * properties they all rely on are pinned here once: the three outcomes, transient coordination read failures
 * being retried rather than propagated, a non-transient failure NOT being swallowed, the per-poll action, and
 * interrupt propagation. Intervals are 0ms (or tiny) so nothing actually sleeps.
 */
public class SmartSnapshotPollingTest {

    private static final String PREFIX = "Smart snapshot: [role=test]";
    private static final Duration GENEROUS = Duration.ofSeconds(60);

    @Test
    public void returnsReadyWhenTheConditionIsSatisfied() throws Exception {
        AtomicInteger polls = new AtomicInteger();

        SmartSnapshotPolling.Outcome outcome = SmartSnapshotPolling.pollUntil(
                PREFIX, "the condition", GENEROUS, Duration.ZERO,
                () -> {
                    // not ready on the first poll, ready on the second -> the loop iterates once
                    return polls.incrementAndGet() < 2 ? SmartSnapshotPolling.PollResult.CONTINUE : SmartSnapshotPolling.PollResult.READY;
                },
                null);

        assertThat(outcome).isEqualTo(SmartSnapshotPolling.Outcome.READY);
        assertThat(polls.get()).isEqualTo(2);
    }

    @Test
    public void returnsAbortedWithoutWaitingOutTheTimeout() throws Exception {
        AtomicInteger polls = new AtomicInteger();

        SmartSnapshotPolling.Outcome outcome = SmartSnapshotPolling.pollUntil(
                PREFIX, "the condition", GENEROUS, Duration.ZERO,
                () -> {
                    polls.incrementAndGet();
                    return SmartSnapshotPolling.PollResult.ABORT;
                },
                null);

        // ABORT means waiting is pointless (restart signaled, newer epoch): stop immediately, do not sit on the
        // generous timeout above
        assertThat(outcome).isEqualTo(SmartSnapshotPolling.Outcome.ABORTED);
        assertThat(polls.get()).isEqualTo(1);
    }

    @Test
    public void returnsTimedOutWhenTheConditionNeverHolds() throws Exception {
        AtomicInteger polls = new AtomicInteger();

        // a 0ms timeout: Threads.timer expires on elapsed > timeout, so the condition gets at least one chance
        // (that is what the leader's 0ms-timeout tests rely on) and then the wait ends instead of blocking
        SmartSnapshotPolling.Outcome outcome = SmartSnapshotPolling.pollUntil(
                PREFIX, "the condition", Duration.ZERO, Duration.ZERO,
                () -> {
                    polls.incrementAndGet();
                    return SmartSnapshotPolling.PollResult.CONTINUE;
                },
                null);

        assertThat(outcome).isEqualTo(SmartSnapshotPolling.Outcome.TIMED_OUT);
        assertThat(polls.get()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void retriesTransientCoordinationReadFailures() throws Exception {
        AtomicInteger polls = new AtomicInteger();

        SmartSnapshotPolling.Outcome outcome = SmartSnapshotPolling.pollUntil(
                PREFIX, "the condition", GENEROUS, Duration.ZERO,
                () -> {
                    if (polls.incrementAndGet() == 1) {
                        // a broker blip on the coordination read; callers rely on this being retried instead of
                        // failing the task (and, for the leader, instead of dropping the held table locks)
                        throw new DebeziumException("read blip");
                    }
                    return SmartSnapshotPolling.PollResult.READY;
                },
                null);

        assertThat(outcome).isEqualTo(SmartSnapshotPolling.Outcome.READY);
        assertThat(polls.get()).isEqualTo(2);
    }

    @Test
    public void doesNotSwallowNonTransientFailures() {
        // only DebeziumException is treated as "not ready yet"; anything else is a real bug/failure and must reach
        // the caller so the round fails instead of silently spinning until the timeout
        assertThatThrownBy(() -> SmartSnapshotPolling.pollUntil(
                PREFIX, "the condition", GENEROUS, Duration.ZERO,
                () -> {
                    throw new IllegalStateException("boom");
                },
                null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom");
    }

    @Test
    public void runsThePerPollActionOncePerIterationAndPropagatesItsFailure() throws Exception {
        AtomicInteger polls = new AtomicInteger();
        AtomicInteger afterEachPoll = new AtomicInteger();

        SmartSnapshotPolling.Outcome outcome = SmartSnapshotPolling.pollUntil(
                PREFIX, "the condition", GENEROUS, Duration.ZERO,
                () -> polls.incrementAndGet() < 3 ? SmartSnapshotPolling.PollResult.CONTINUE : SmartSnapshotPolling.PollResult.READY,
                afterEachPoll::incrementAndGet);

        // this is the leader's keepAlive(): once per non-terminal iteration, never after the terminal poll
        assertThat(outcome).isEqualTo(SmartSnapshotPolling.Outcome.READY);
        assertThat(polls.get()).isEqualTo(3);
        assertThat(afterEachPoll.get()).isEqualTo(2);

        // the per-poll action is NOT inside the transient-retry, so a dead held connection (keepAlive throwing)
        // ends the wait and fails the round
        assertThatThrownBy(() -> SmartSnapshotPolling.pollUntil(
                PREFIX, "the condition", GENEROUS, Duration.ZERO,
                () -> SmartSnapshotPolling.PollResult.CONTINUE,
                () -> {
                    throw new DebeziumException("snapshot-holder connection is dead");
                }))
                .isInstanceOf(DebeziumException.class)
                .hasMessage("snapshot-holder connection is dead");
    }

    @Test
    public void propagatesAnInterruptSoTheTaskStopPathEndsTheWait() {
        // the park between polls is what makes the loop interrupt-aware. The interval must be > 0: with a 0 period
        // the parker returns without ever checking the interrupt flag.
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> SmartSnapshotPolling.pollUntil(
                    PREFIX, "the condition", GENEROUS, Duration.ofMillis(10),
                    () -> SmartSnapshotPolling.PollResult.CONTINUE,
                    null))
                    .isInstanceOf(InterruptedException.class);
        }
        finally {
            // clear so the interrupt does not leak into other tests on this thread
            Thread.interrupted();
        }
    }
}
