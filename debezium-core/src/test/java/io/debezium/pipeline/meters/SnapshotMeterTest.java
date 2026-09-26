/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.TaskStateMetrics;
import io.debezium.util.Clock;

/**
 * Tests for {@link SnapshotMeter} DND delay behavior using timestamp-based lazy evaluation.
 */
public class SnapshotMeterTest {

    private static final long DND_DELAY_MS = 600_000L;

    private final AtomicLong currentTime = new AtomicLong(1_000_000L);
    private final Clock testClock = currentTime::get;

    private TaskStateMetrics taskStateMetrics;

    @Before
    public void setUp() {
        TestConnectorConfig config = new TestConnectorConfig(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "test")
                .build());
        CdcSourceTaskContext taskContext = new CdcSourceTaskContext(config,
                "0", Collections.emptyMap(), Collections::emptyList);
        taskStateMetrics = new TaskStateMetrics(taskContext, testClock) {
            @Override
            public void register() {
            }

            @Override
            public void unregister() {
            }
        };
    }

    // --- Legacy mode (smartSnapshot=false): DND activates immediately ---

    @Test
    public void testLegacyModeDndSetImmediatelyOnSnapshotStart() {
        SnapshotMeter meter = new SnapshotMeter(testClock, taskStateMetrics, false, 0L);

        meter.snapshotStarted();

        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(1L);
    }

    @Test
    public void testLegacyModeDndClearedOnSnapshotCompleted() {
        SnapshotMeter meter = new SnapshotMeter(testClock, taskStateMetrics, false, 0L);

        meter.snapshotStarted();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(1L);

        meter.snapshotCompleted();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(0L);
    }

    @Test
    public void testLegacyModePauseAndResume() {
        SnapshotMeter meter = new SnapshotMeter(testClock, taskStateMetrics, false, 0L);

        meter.snapshotStarted();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(1L);

        meter.snapshotPaused();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(0L);

        meter.snapshotResumed();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(1L);
    }

    @Test
    public void testDefaultConstructorUsesLegacyMode() {
        SnapshotMeter meter = new SnapshotMeter(testClock, taskStateMetrics, false, 0L);

        meter.snapshotStarted();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(1L);
    }

    // --- Smart mode: DND activates only after delay elapses ---

    @Test
    public void testSmartModeDndNotSetImmediately() {
        SnapshotMeter meter = new SnapshotMeter(testClock, taskStateMetrics, true, DND_DELAY_MS);

        meter.snapshotStarted();

        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should be 0 immediately — delay has not elapsed")
                .isEqualTo(0L);
    }

    @Test
    public void testSmartModeDndActivatesAfterDelay() {
        SnapshotMeter meter = new SnapshotMeter(testClock, taskStateMetrics, true, DND_DELAY_MS);

        meter.snapshotStarted();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(0L);

        // Advance clock past the delay
        currentTime.addAndGet(DND_DELAY_MS + 1);

        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should be 1 after delay elapses")
                .isEqualTo(1L);
    }

    @Test
    public void testSmartModeSnapshotCompletesBeforeDelay() {
        SnapshotMeter meter = new SnapshotMeter(testClock, taskStateMetrics, true, DND_DELAY_MS);

        meter.snapshotStarted();
        meter.snapshotCompleted();

        // Advance clock past the delay
        currentTime.addAndGet(DND_DELAY_MS + 1);

        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should remain 0 — cleared before delay elapsed")
                .isEqualTo(0L);
    }

    @Test
    public void testSmartModePauseClearsDnd() {
        SnapshotMeter meter = new SnapshotMeter(testClock, taskStateMetrics, true, DND_DELAY_MS);

        meter.snapshotStarted();

        // Advance past delay so DND would be active
        currentTime.addAndGet(DND_DELAY_MS + 1);
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(1L);

        meter.snapshotPaused();
        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should be 0 after pause")
                .isEqualTo(0L);
    }

    @Test
    public void testSmartModeResumeResetsDelay() {
        SnapshotMeter meter = new SnapshotMeter(testClock, taskStateMetrics, true, DND_DELAY_MS);

        meter.snapshotStarted();
        meter.snapshotPaused();

        meter.snapshotResumed();
        assertThat(taskStateMetrics.getConnectTaskDnd())
                .isEqualTo(1L);
    }

    @Test
    public void testSmartModeAbortClearsDnd() {
        SnapshotMeter meter = new SnapshotMeter(testClock, taskStateMetrics, true, DND_DELAY_MS);

        meter.snapshotStarted();
        meter.snapshotAborted();

        currentTime.addAndGet(DND_DELAY_MS + 1);
        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should be 0 after abort")
                .isEqualTo(0L);
    }

    @Test
    public void testSmartModeSkipClearsDnd() {
        SnapshotMeter meter = new SnapshotMeter(testClock, taskStateMetrics, true, DND_DELAY_MS);

        meter.snapshotStarted();
        meter.snapshotSkipped();

        currentTime.addAndGet(DND_DELAY_MS + 1);
        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should be 0 after skip")
                .isEqualTo(0L);
    }

    @Test
    public void testSmartModeCompletedAfterDndAlreadyActive() {
        SnapshotMeter meter = new SnapshotMeter(testClock, taskStateMetrics, true, DND_DELAY_MS);

        meter.snapshotStarted();
        currentTime.addAndGet(DND_DELAY_MS + 1);
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(1L);

        meter.snapshotCompleted();
        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should be 0 after completion even if it was already active")
                .isEqualTo(0L);
    }

    // --- Test infrastructure ---

    private static class TestConnectorConfig extends CommonConnectorConfig {
        protected TestConnectorConfig(Configuration config) {
            super(config, 0);
        }

        @Override
        public String getContextName() {
            return "test";
        }

        @Override
        public String getConnectorName() {
            return "test";
        }

        @Override
        public EnumeratedValue getSnapshotMode() {
            return null;
        }

        @Override
        public Optional<EnumeratedValue> getSnapshotLockingMode() {
            return Optional.empty();
        }

        @Override
        protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
            return null;
        }
    }
}
