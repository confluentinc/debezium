/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.util.Clock;

/**
 * Exercises {@link SnapshotMeter}'s four state attributes across lifecycle transitions and
 * asserts that the boolean ({@code is*}) and numeric ({@code get*}) getters never disagree.
 * Both shapes back onto the same {@code AtomicBoolean}, so a divergence would indicate a
 * regression in the impl rather than a flaky test.
 */
public class SnapshotMeterTest {

    @Test
    public void freshMeterAllStatesFalse() {
        SnapshotMeter meter = new SnapshotMeter(Clock.system());

        assertBothShapesAgree(meter, false, false, false, false);
    }

    @Test
    public void snapshotStartedRunningOnly() {
        SnapshotMeter meter = new SnapshotMeter(Clock.system());
        meter.snapshotStarted();

        assertBothShapesAgree(meter, true, false, false, false);
    }

    @Test
    public void snapshotPausedRunningClearedPausedSet() {
        SnapshotMeter meter = new SnapshotMeter(Clock.system());
        meter.snapshotStarted();
        meter.snapshotPaused();

        assertBothShapesAgree(meter, false, true, false, false);
    }

    @Test
    public void snapshotResumedReturnsToRunning() {
        SnapshotMeter meter = new SnapshotMeter(Clock.system());
        meter.snapshotStarted();
        meter.snapshotPaused();
        meter.snapshotResumed();

        assertBothShapesAgree(meter, true, false, false, false);
    }

    @Test
    public void snapshotCompletedClearsRunningSetsCompleted() {
        SnapshotMeter meter = new SnapshotMeter(Clock.system());
        meter.snapshotStarted();
        meter.snapshotCompleted();

        assertBothShapesAgree(meter, false, false, true, false);
    }

    @Test
    public void snapshotAbortedClearsRunningSetsAborted() {
        SnapshotMeter meter = new SnapshotMeter(Clock.system());
        meter.snapshotStarted();
        meter.snapshotAborted();

        assertBothShapesAgree(meter, false, false, false, true);
    }

    @Test
    public void resetClearsAllStates() {
        SnapshotMeter meter = new SnapshotMeter(Clock.system());
        meter.snapshotStarted();
        meter.snapshotCompleted();
        meter.reset();

        assertBothShapesAgree(meter, false, false, false, false);
    }

    private static void assertBothShapesAgree(SnapshotMeter meter, boolean running, boolean paused,
                                              boolean completed, boolean aborted) {
        assertThat(meter.isSnapshotRunning()).isEqualTo(running);
        assertThat(meter.isSnapshotPaused()).isEqualTo(paused);
        assertThat(meter.isSnapshotCompleted()).isEqualTo(completed);
        assertThat(meter.isSnapshotAborted()).isEqualTo(aborted);

        assertThat(meter.getSnapshotRunning()).isEqualTo(running ? 1L : 0L);
        assertThat(meter.getSnapshotPaused()).isEqualTo(paused ? 1L : 0L);
        assertThat(meter.getSnapshotCompleted()).isEqualTo(completed ? 1L : 0L);
        assertThat(meter.getSnapshotAborted()).isEqualTo(aborted ? 1L : 0L);
    }
}
