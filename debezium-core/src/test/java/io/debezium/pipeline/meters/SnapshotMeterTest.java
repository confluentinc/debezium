/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
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
 * Tests for {@link SnapshotMeter} DND delay behavior using a controllable scheduler.
 */
public class SnapshotMeterTest {

    private SnapshotMeter snapshotMeter;
    private TaskStateMetrics taskStateMetrics;
    private ManualScheduler manualScheduler;

    @Before
    public void setUp() {
        TestConnectorConfig config = new TestConnectorConfig(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "test")
                .build());
        CdcSourceTaskContext taskContext = new CdcSourceTaskContext(config,
                "0", Collections.emptyMap(), Collections::emptyList);
        taskStateMetrics = new TaskStateMetrics(taskContext) {
            @Override
            public void register() {
            }

            @Override
            public void unregister() {
            }
        };
        manualScheduler = new ManualScheduler();
    }

    @After
    public void tearDown() {
        if (snapshotMeter != null) {
            snapshotMeter.close();
        }
    }

    // --- Legacy mode (smartSnapshot=false) ---

    @Test
    public void testLegacyModeDndSetImmediatelyOnSnapshotStart() {
        snapshotMeter = new SnapshotMeter(Clock.system(), taskStateMetrics, false, 0L);

        snapshotMeter.snapshotStarted();

        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(1L);
    }

    @Test
    public void testLegacyModeDndClearedOnSnapshotCompleted() {
        snapshotMeter = new SnapshotMeter(Clock.system(), taskStateMetrics, false, 0L);

        snapshotMeter.snapshotStarted();
        snapshotMeter.snapshotCompleted();

        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(0L);
    }

    @Test
    public void testLegacyModeDndResetOnPauseAndSetOnResume() {
        snapshotMeter = new SnapshotMeter(Clock.system(), taskStateMetrics, false, 0L);

        snapshotMeter.snapshotStarted();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(1L);

        snapshotMeter.snapshotPaused();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(0L);

        snapshotMeter.snapshotResumed();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(1L);
    }

    @Test
    public void testDefaultConstructorUsesLegacyMode() {
        snapshotMeter = new SnapshotMeter(Clock.system(), taskStateMetrics);

        snapshotMeter.snapshotStarted();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(1L);
    }

    // --- Smart mode: DND is NOT set immediately, only after scheduler fires ---

    @Test
    public void testSmartModeDndNotSetImmediately() {
        snapshotMeter = createSmartMeter();

        snapshotMeter.snapshotStarted();

        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should not be set immediately")
                .isEqualTo(0L);
        assertThat(manualScheduler.pendingTasks()).hasSize(1);
    }

    @Test
    public void testSmartModeDndSetWhenSchedulerFires() {
        snapshotMeter = createSmartMeter();

        snapshotMeter.snapshotStarted();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(0L);

        // Simulate the delay elapsing
        manualScheduler.runPending();

        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should be 1 after scheduler fires")
                .isEqualTo(1L);
    }

    @Test
    public void testSmartModeSnapshotCompletesBeforeDelayFires() {
        snapshotMeter = createSmartMeter();

        snapshotMeter.snapshotStarted();
        snapshotMeter.snapshotCompleted();

        // The pending task was cancelled, so firing should have no effect
        manualScheduler.runPending();

        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should remain 0 — cancelled task must not set DND")
                .isEqualTo(0L);
    }

    @Test
    public void testSmartModePauseCancelsPendingDnd() {
        snapshotMeter = createSmartMeter();

        snapshotMeter.snapshotStarted();
        assertThat(manualScheduler.pendingTasks()).hasSize(1);

        snapshotMeter.snapshotPaused();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(0L);

        // The task was cancelled; running it should have no effect
        manualScheduler.runPending();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(0L);
    }

    @Test
    public void testSmartModeResumeSchedulesNewTask() {
        snapshotMeter = createSmartMeter();

        snapshotMeter.snapshotStarted();
        snapshotMeter.snapshotPaused();
        manualScheduler.clear();

        snapshotMeter.snapshotResumed();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(0L);
        assertThat(manualScheduler.pendingTasks()).hasSize(1);

        // Fire the new scheduled task
        manualScheduler.runPending();
        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should be 1 after resumed delay fires")
                .isEqualTo(1L);
    }

    @Test
    public void testSmartModeAbortCancelsPendingDnd() {
        snapshotMeter = createSmartMeter();

        snapshotMeter.snapshotStarted();
        snapshotMeter.snapshotAborted();

        manualScheduler.runPending();
        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should stay 0 — abort cancels pending and doesn't change DND")
                .isEqualTo(0L);
    }

    @Test
    public void testSmartModeSkipCancelsPendingDnd() {
        snapshotMeter = createSmartMeter();

        snapshotMeter.snapshotStarted();
        snapshotMeter.snapshotSkipped();

        manualScheduler.runPending();
        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should be 0 after skip")
                .isEqualTo(0L);
    }

    @Test
    public void testSmartModeCompletedAfterDndAlreadyFired() {
        snapshotMeter = createSmartMeter();

        snapshotMeter.snapshotStarted();
        manualScheduler.runPending();
        assertThat(taskStateMetrics.getConnectTaskDnd()).isEqualTo(1L);

        snapshotMeter.snapshotCompleted();
        assertThat(taskStateMetrics.getConnectTaskDnd())
                .as("DND should be 0 after completion, even if it was already set")
                .isEqualTo(0L);
    }

    private SnapshotMeter createSmartMeter() {
        return new SnapshotMeter(Clock.system(), taskStateMetrics, true, 600000L, manualScheduler);
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

    /**
     * A manually-controlled ScheduledExecutorService that captures scheduled tasks
     * and lets tests fire them on demand — no real timing involved.
     */
    private static class ManualScheduler implements ScheduledExecutorService {

        private final List<ManualFuture> tasks = new ArrayList<>();

        List<ManualFuture> pendingTasks() {
            List<ManualFuture> pending = new ArrayList<>();
            for (ManualFuture t : tasks) {
                if (!t.cancelled) {
                    pending.add(t);
                }
            }
            return pending;
        }

        void runPending() {
            for (ManualFuture t : tasks) {
                if (!t.cancelled) {
                    t.command.run();
                }
            }
        }

        void clear() {
            tasks.clear();
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            ManualFuture future = new ManualFuture(command);
            tasks.add(future);
            return future;
        }

        // --- Unused methods below ---
        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<?> submit(Runnable task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command) {
            throw new UnsupportedOperationException();
        }

        private static class ManualFuture implements ScheduledFuture<Void> {
            final Runnable command;
            boolean cancelled = false;

            ManualFuture(Runnable command) {
                this.command = command;
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                cancelled = true;
                return true;
            }

            @Override
            public boolean isCancelled() {
                return cancelled;
            }

            @Override
            public boolean isDone() {
                return cancelled;
            }

            @Override
            public Void get() throws InterruptedException, ExecutionException {
                return null;
            }

            @Override
            public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                return null;
            }

            @Override
            public long getDelay(TimeUnit unit) {
                return 0;
            }

            @Override
            public int compareTo(Delayed o) {
                return 0;
            }
        }
    }
}
