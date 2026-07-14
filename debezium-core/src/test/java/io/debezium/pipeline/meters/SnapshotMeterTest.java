/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import io.debezium.pipeline.metrics.TaskStateMetrics;
import io.debezium.util.Clock;

public class SnapshotMeterTest {

    @Test
    public void lifecycleMethodsDoNotTouchDnd() {
        TaskStateMetrics taskStateMetrics = mock(TaskStateMetrics.class);
        SnapshotMeter meter = new SnapshotMeter(Clock.SYSTEM, taskStateMetrics);

        meter.snapshotStarted();
        meter.snapshotPaused();
        meter.snapshotResumed();
        meter.snapshotCompleted();
        meter.snapshotAborted();
        meter.snapshotSkipped();

        verify(taskStateMetrics, never()).setConnectTaskDnd(anyLong());
    }

    @Test
    public void stillTracksSnapshotRunning() {
        SnapshotMeter meter = new SnapshotMeter(Clock.SYSTEM, mock(TaskStateMetrics.class));

        meter.snapshotStarted();
        assertThat(meter.getSnapshotRunning()).isEqualTo(1L);

        meter.snapshotCompleted();
        assertThat(meter.getSnapshotRunning()).isEqualTo(0L);
    }
}
