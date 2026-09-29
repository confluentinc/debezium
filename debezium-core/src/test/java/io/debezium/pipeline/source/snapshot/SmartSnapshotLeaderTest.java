/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.TableId;

/**
 * Unit tests for the leader (task-0) snapshot-preparation orchestration extracted from.
 * A mock lifecycle + coordination let us assert the sequence without a
 * database or Kafka. pollMs = 0 so the wait loop does not sleep.
 */
public class SmartSnapshotLeaderTest {

    private static final int EPOCH = 3;
    private static final List<TableId> TABLES = List.of(
            new TableId(null, "public", "a"),
            new TableId(null, "public", "b"));

    @Mock
    private SmartSnapshotLifecycleManager lifecycle;

    @Mock
    private SnapshotCoordinationFacade coordination;

    @Mock
    private ErrorHandler errorHandler;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);
    }

    private SmartSnapshotLeader prep(int numTasks, boolean shouldStream) {
        // generous timeouts so the wait loops exit on the mocked conditions, not on the clock
        return prep(numTasks, shouldStream, 60_000L, 60_000L);
    }

    private SmartSnapshotLeader prep(int numTasks, boolean shouldStream, long joinWaitTimeoutMs, long startedTransactionTimeoutMs) {
        return leader(numTasks, shouldStream, 0L, joinWaitTimeoutMs, startedTransactionTimeoutMs);
    }

    // The leader creates its own coordination facade in run(); hand it the mock instead of a Kafka-backed one.
    private SmartSnapshotLeader leader(int numTasks, boolean shouldStream, long pollMs, long joinWaitTimeoutMs, long startedTransactionTimeoutMs) {
        return new SmartSnapshotLeader(lifecycle, errorHandler, EPOCH, numTasks, shouldStream, null, null, null,
                pollMs, joinWaitTimeoutMs, startedTransactionTimeoutMs, () -> {
                }) {
            @Override
            SnapshotCoordinationFacade createCoordination() {
                return coordination;
            }
        };
    }

    private void allTasksJoined(int numTasks) {
        when(coordination.allTasksJoined(numTasks, EPOCH)).thenReturn(true);
    }

    @Test
    public void skipsPreparationWhenLeaderAlreadyCompleted() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(coordination).start(SnapshotCoordination.MissingTopicPolicy.FAIL);
        verify(lifecycle, never()).prepareSnapshot(anyBoolean());
        verify(coordination, never()).writeSnapshotInfo(any(), any(), any(), eq(EPOCH), any(), eq(2));
    }

    @Test
    public void skipsPreparationWhenRestartSignalled() {
        // a restart already flagged for this epoch is caught by the join wait's first poll, before any preparation
        when(coordination.anyRestartNeeded(2, EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(coordination).start(SnapshotCoordination.MissingTopicPolicy.FAIL);
        verify(lifecycle, never()).prepareSnapshot(anyBoolean());
        verify(coordination, never()).writeSnapshotInfo(any(), any(), any(), eq(EPOCH), any(), eq(2));
    }

    @Test
    public void preparesPublishesAndReleasesWhenAllTasksJoin() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        allTasksJoined(2);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", 99L, TABLES));
        when(coordination.allTasksStartedTransaction(2, EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(lifecycle).prepareSnapshot(true);
        verify(coordination).writeSnapshotInfo("snap", "0/16B3748", 99L, EPOCH, TABLES, 2);
        verify(lifecycle).onAllTasksStartedTransaction();
        // one release for every path out of run(), including the success path: the held connections are dropped
        // as soon as the leader thread ends rather than lingering until the task is stopped
        verify(lifecycle).releaseSnapshot();
    }

    @Test
    public void waitsForAllTasksToJoinBeforePreparing() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        // not every task has joined on the first check, all have on the second -> the join wait runs one iteration first
        when(coordination.allTasksJoined(2, EPOCH)).thenReturn(false, true);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", 99L, TABLES));
        when(coordination.allTasksStartedTransaction(2, EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(lifecycle).prepareSnapshot(true);
        verify(lifecycle).onAllTasksStartedTransaction();
    }

    @Test
    public void joinTimeoutFailsTaskWithoutPreparingOrBumpingEpoch() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        // task-1 never joins; with a 0ms join timeout the leader must not take any locks
        when(coordination.allTasksJoined(2, EPOCH)).thenReturn(false);

        prep(2, true, 0L, 60_000L).run();

        verify(lifecycle, never()).prepareSnapshot(anyBoolean());
        verify(coordination, never()).writeSnapshotInfo(any(), any(), any(), eq(EPOCH), any(), eq(2));
        // nothing was prepared/published, so no epoch bump -> fail the task instead
        verify(coordination, never()).writeRestartNeeded(any(), eq(EPOCH));
        verify(errorHandler).setProducerThrowable(any(DebeziumException.class));
        // nothing was held, so this release is a no-op; it runs because run() releases on every path
        verify(lifecycle).releaseSnapshot();

        // the join timeout is reported only after this thread's coordination facade is closed, so the restart it
        // triggers cannot interrupt that close mid-way (same deferral as the post-publish failure path)
        InOrder order = inOrder(coordination, errorHandler);
        order.verify(coordination).stop();
        order.verify(errorHandler).setProducerThrowable(any(DebeziumException.class));
    }

    @Test
    public void keepsSnapshotAliveWhileWaitingForTasks() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        allTasksJoined(1);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", 99L, TABLES));
        // not started on the first check, started afterwards -> the wait loop runs one iteration
        when(coordination.allTasksStartedTransaction(1, EPOCH)).thenReturn(false, true);

        prep(1, true).run();

        verify(lifecycle, times(1)).keepAlive();
        verify(lifecycle).onAllTasksStartedTransaction();
    }

    @Test
    public void startedTransactionTimeoutReleasesAndSignalsRestart() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        allTasksJoined(2);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", 99L, TABLES));
        // tasks joined but never start their transaction; 0ms timeout ends the held critical section
        when(coordination.allTasksStartedTransaction(2, EPOCH)).thenReturn(false);

        prep(2, true, 60_000L, 0L).run();

        verify(lifecycle).prepareSnapshot(true);
        verify(coordination).writeSnapshotInfo("snap", "0/16B3748", 99L, EPOCH, TABLES, 2);
        verify(lifecycle).releaseSnapshot();
        verify(coordination).writeRestartNeeded("0", EPOCH);
        verify(lifecycle, never()).onAllTasksStartedTransaction();
    }

    @Test
    public void restartSignalledDuringStartedTransactionWaitReleasesWithoutSignallingAgain() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        allTasksJoined(2);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", 99L, TABLES));
        // no restart during the join wait; task-1 signals one after the snapshot is published
        when(coordination.anyRestartNeeded(2, EPOCH)).thenReturn(false, true);
        when(coordination.allTasksStartedTransaction(2, EPOCH)).thenReturn(false);

        prep(2, true).run();

        verify(coordination).writeSnapshotInfo("snap", "0/16B3748", 99L, EPOCH, TABLES, 2);
        verify(lifecycle).releaseSnapshot();
        verify(lifecycle, never()).onAllTasksStartedTransaction();
        // the restart is already on the topic; the leader must not write a second one
        verify(coordination, never()).writeRestartNeeded(any(), eq(EPOCH));
        verify(errorHandler, never()).setProducerThrowable(any());
    }

    @Test
    public void transientReadFailureDuringStartedTransactionWaitIsToleratedNotAborted() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        allTasksJoined(2);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", 99L, TABLES));
        // the read blips once (broker hiccup) then reports started. While the table locks are held a transient
        // read failure must be retried, NOT treated as a round abort that drops the locks and discards the snapshot.
        when(coordination.allTasksStartedTransaction(2, EPOCH)).thenThrow(new DebeziumException("read blip")).thenReturn(true);

        prep(2, true).run();

        verify(lifecycle).onAllTasksStartedTransaction();
        verify(coordination, never()).writeRestartNeeded(any(), eq(EPOCH));
        verify(errorHandler, never()).setProducerThrowable(any());
    }

    @Test
    public void transientReadFailureDuringJoinWaitIsTolerated() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        // the read blips once then reports joined; the join wait must retry instead of failing the task.
        when(coordination.allTasksJoined(2, EPOCH)).thenThrow(new DebeziumException("read blip")).thenReturn(true);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", 99L, TABLES));
        when(coordination.allTasksStartedTransaction(2, EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(lifecycle).prepareSnapshot(true);
        verify(lifecycle).onAllTasksStartedTransaction();
        verify(errorHandler, never()).setProducerThrowable(any());
    }

    @Test
    public void coordinationCreationFailureFailsTheTaskWithoutPreparing() {
        SmartSnapshotLeader leader = new SmartSnapshotLeader(lifecycle, errorHandler, EPOCH, 2, true, null, null, null,
                0L, 60_000L, 60_000L, () -> {
                }) {
            @Override
            SnapshotCoordinationFacade createCoordination() {
                throw new DebeziumException("bad coordination client config");
            }
        };

        leader.run();

        // nothing was created, so there is nothing to stop; the cleanup must not trip over the missing facade
        verify(lifecycle, never()).prepareSnapshot(anyBoolean());
        verify(lifecycle).releaseSnapshot();
        verify(errorHandler).setProducerThrowable(any(DebeziumException.class));
    }

    @Test
    public void onPreparationFailureReleasesAndFailsTheTask() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        allTasksJoined(2);
        when(lifecycle.prepareSnapshot(anyBoolean())).thenThrow(new RuntimeException("boom"));

        prep(2, true).run();

        verify(lifecycle).releaseSnapshot();
        verify(errorHandler).setProducerThrowable(any(DebeziumException.class));
        verify(lifecycle, never()).onAllTasksStartedTransaction();
    }

    @Test
    public void keepAliveFailureAfterPublishReleasesSignalsRestartAndFailsAfterCleanup() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        allTasksJoined(2);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", 99L, TABLES));
        // tasks never start their transaction, and keepAlive fails because the DB connection was killed
        when(coordination.allTasksStartedTransaction(2, EPOCH)).thenReturn(false);
        doThrow(new DebeziumException("snapshot-holder connection is dead")).when(lifecycle).keepAlive();

        prep(2, true).run();

        // the snapshot was already published, so the round is invalidated and the task is failed
        verify(lifecycle).releaseSnapshot();
        verify(coordination).writeRestartNeeded("0", EPOCH);
        verify(errorHandler).setProducerThrowable(any(DebeziumException.class));
        verify(lifecycle, never()).onAllTasksStartedTransaction();

        // the deferred-failure fix: the leader closes its own coordination facade BEFORE failing the task,
        // so the restart the failure triggers cannot interrupt that close mid-way
        InOrder order = inOrder(coordination, errorHandler);
        order.verify(coordination).stop();
        order.verify(errorHandler).setProducerThrowable(any(DebeziumException.class));
    }

    @Test
    public void interruptDuringJoinWaitReleasesSnapshot() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        // never joined, so the join-wait loop reaches metronome.pause(), which throws on the pre-set interrupt.
        // pollMs must be > 0 here: with a 0 period, parker.pause() returns without ever checking the interrupt.
        when(coordination.allTasksJoined(2, EPOCH)).thenReturn(false);
        SmartSnapshotLeader leader = leader(2, true, 10L, 60_000L, 60_000L);

        Thread.currentThread().interrupt();
        try {
            leader.run();

            // Path A must release the held snapshot defensively, must not fail the task (clean shutdown),
            // and must leave the interrupt flag restored
            verify(lifecycle).releaseSnapshot();
            verify(lifecycle, never()).onAllTasksStartedTransaction();
            verify(errorHandler, never()).setProducerThrowable(any());
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        }
        finally {
            // clear so the interrupt does not leak into other tests on this thread
            Thread.interrupted();
        }
    }

    @Test
    public void nonStreamingModePreparesWithoutSlot() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        allTasksJoined(1);
        when(lifecycle.prepareSnapshot(false)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", 99L, TABLES));
        when(coordination.allTasksStartedTransaction(1, EPOCH)).thenReturn(true);

        prep(1, false).run();

        verify(lifecycle).prepareSnapshot(false);
        verify(lifecycle).onAllTasksStartedTransaction();
    }

    // Here the leader thread is blocked in a call that ignores interrupts (like a JDBC call) and only ends when
    // releaseSnapshot aborts it. stop() must interrupt it first (so the aborted call reads as a shutdown), release,
    // and then wait for the thread to actually end, so its own cleanup has run by the time stop() returns.
    @Test
    public void stopInterruptsReleasesAndWaitsForTheLeaderThreadToEnd() throws Exception {
        CountDownLatch released = new CountDownLatch(1);
        CountDownLatch running = new CountDownLatch(1);
        AtomicBoolean interruptedBeforeRelease = new AtomicBoolean(false);
        AtomicReference<Thread> leaderThread = new AtomicReference<>();

        // releaseSnapshot is what ends the blocked leader thread, standing in for aborting the connection
        doAnswer(inv -> {
            // give the interrupt time to land before the release unblocks the thread
            Thread.sleep(50);
            released.countDown();
            return null;
        }).when(lifecycle).releaseSnapshot();

        // start() names the thread via the standard convention, so it needs a config with the naming fields.
        CommonConnectorConfig connectorConfig = mock(CommonConnectorConfig.class);
        when(connectorConfig.getLogicalName()).thenReturn("test-server");
        when(connectorConfig.getConnectorThreadNamePattern())
                .thenReturn("${debezium}-${connector.class.simple}-${topic.prefix}-${functionality}");

        SmartSnapshotLeader leader = new SmartSnapshotLeader(lifecycle, errorHandler, EPOCH, 2, true, null, connectorConfig,
                SourceConnector.class, 0L, 60_000L, 60_000L, () -> {
                }) {
            @Override
            public void run() {
                leaderThread.set(Thread.currentThread());
                running.countDown();
                while (released.getCount() > 0) {
                    try {
                        released.await();
                    }
                    catch (InterruptedException e) {
                        // ignore, to model a database call that cannot be interrupted
                        interruptedBeforeRelease.set(released.getCount() > 0);
                    }
                }
            }
        };

        leader.start();
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();

        leader.stop(2000);

        verify(lifecycle).releaseSnapshot();
        assertThat(interruptedBeforeRelease.get()).isTrue();
        // the key property: stop() returned only after the leader thread had finished
        assertThat(leaderThread.get().isAlive()).isFalse();
    }

    @Test
    public void stopWithoutStartOnlyReleases() {
        // e.g. the task is stopped after creating the leader but before starting it: nothing to interrupt or join
        leader(2, true, 0L, 60_000L, 60_000L).stop(2000);

        verify(lifecycle).releaseSnapshot();
    }
}
