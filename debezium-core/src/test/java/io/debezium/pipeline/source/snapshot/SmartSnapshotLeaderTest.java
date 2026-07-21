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
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.DebeziumException;
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
        return new SmartSnapshotLeader(lifecycle, coordination, errorHandler, EPOCH, numTasks, shouldStream,
                0L, joinWaitTimeoutMs, startedTransactionTimeoutMs, () -> {
                });
    }

    private void allTasksJoined(int numTasks) {
        for (int i = 0; i < numTasks; i++) {
            when(coordination.isTaskJoined(String.valueOf(i), EPOCH)).thenReturn(true);
        }
    }

    @Test
    public void skipsPreparationWhenLeaderAlreadyCompleted() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(coordination).start(SnapshotCoordination.MissingTopicPolicy.FAIL);
        verify(lifecycle, never()).prepareSnapshot(anyBoolean());
        verify(coordination, never()).writeSnapshotInfo(any(), any(), eq(EPOCH), any(), eq(2));
    }

    @Test
    public void skipsPreparationWhenRestartSignalled() {
        when(coordination.isRestartNeeded("1", EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(coordination).start(SnapshotCoordination.MissingTopicPolicy.FAIL);
        verify(lifecycle, never()).prepareSnapshot(anyBoolean());
        verify(coordination, never()).writeSnapshotInfo(any(), any(), eq(EPOCH), any(), eq(2));
    }

    @Test
    public void preparesPublishesAndReleasesWhenAllTasksJoin() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        allTasksJoined(2);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", TABLES));
        when(coordination.isTaskStartedTransaction("0", EPOCH)).thenReturn(true);
        when(coordination.isTaskStartedTransaction("1", EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(lifecycle).prepareSnapshot(true);
        verify(coordination).writeSnapshotInfo("snap", "0/16B3748", EPOCH, TABLES, 2);
        verify(lifecycle).onAllTasksStartedTransaction();
        verify(lifecycle, never()).releaseSnapshot();
    }

    @Test
    public void waitsForAllTasksToJoinBeforePreparing() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        // task-0 joined, task-1 joins only on the second check -> the join wait runs one iteration first
        when(coordination.isTaskJoined("0", EPOCH)).thenReturn(true);
        when(coordination.isTaskJoined("1", EPOCH)).thenReturn(false, true);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", TABLES));
        when(coordination.isTaskStartedTransaction("0", EPOCH)).thenReturn(true);
        when(coordination.isTaskStartedTransaction("1", EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(lifecycle).prepareSnapshot(true);
        verify(lifecycle).onAllTasksStartedTransaction();
    }

    @Test
    public void joinTimeoutFailsTaskWithoutPreparingOrBumpingEpoch() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        // task-1 never joins; with a 0ms join timeout the leader must not take any locks
        when(coordination.isTaskJoined("0", EPOCH)).thenReturn(true);
        when(coordination.isTaskJoined("1", EPOCH)).thenReturn(false);

        prep(2, true, 0L, 60_000L).run();

        verify(lifecycle, never()).prepareSnapshot(anyBoolean());
        verify(coordination, never()).writeSnapshotInfo(any(), any(), eq(EPOCH), any(), eq(2));
        // nothing was prepared/published, so no epoch bump -> fail the task instead
        verify(coordination, never()).writeRestartNeeded(any(), eq(EPOCH));
        verify(errorHandler).setProducerThrowable(any(DebeziumException.class));
        verify(lifecycle, never()).releaseSnapshot();
    }

    @Test
    public void keepsSnapshotAliveWhileWaitingForTasks() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        allTasksJoined(1);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", TABLES));
        // not started on the first check, started afterwards -> the wait loop runs one iteration
        when(coordination.isTaskStartedTransaction("0", EPOCH)).thenReturn(false, true);

        prep(1, true).run();

        verify(lifecycle, times(1)).keepAlive();
        verify(lifecycle).onAllTasksStartedTransaction();
    }

    @Test
    public void startedTransactionTimeoutReleasesAndSignalsRestart() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        allTasksJoined(2);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", TABLES));
        // tasks joined but never start their transaction; 0ms timeout ends the held critical section
        when(coordination.isTaskStartedTransaction("0", EPOCH)).thenReturn(false);
        when(coordination.isTaskStartedTransaction("1", EPOCH)).thenReturn(false);

        prep(2, true, 60_000L, 0L).run();

        verify(lifecycle).prepareSnapshot(true);
        verify(coordination).writeSnapshotInfo("snap", "0/16B3748", EPOCH, TABLES, 2);
        verify(lifecycle).releaseSnapshot();
        verify(coordination).writeRestartNeeded("0", EPOCH);
        verify(lifecycle, never()).onAllTasksStartedTransaction();
    }

    @Test
    public void transientReadFailureDuringStartedTransactionWaitIsToleratedNotAborted() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        allTasksJoined(2);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", TABLES));
        // task-0's read blips once (broker hiccup) then reports started. While the table locks are held a transient
        // read failure must be retried, NOT treated as a round abort that drops the locks and discards the snapshot.
        when(coordination.isTaskStartedTransaction("0", EPOCH)).thenThrow(new DebeziumException("read blip")).thenReturn(true);
        when(coordination.isTaskStartedTransaction("1", EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(lifecycle).onAllTasksStartedTransaction();
        verify(lifecycle, never()).releaseSnapshot();
        verify(coordination, never()).writeRestartNeeded(any(), eq(EPOCH));
        verify(errorHandler, never()).setProducerThrowable(any());
    }

    @Test
    public void transientReadFailureDuringJoinWaitIsTolerated() {
        when(coordination.isTaskDone("0", EPOCH)).thenReturn(false);
        when(coordination.isTaskJoined("0", EPOCH)).thenReturn(true);
        // task-1's read blips once then reports joined; the join wait must retry instead of failing the task.
        when(coordination.isTaskJoined("1", EPOCH)).thenThrow(new DebeziumException("read blip")).thenReturn(true);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", TABLES));
        when(coordination.isTaskStartedTransaction("0", EPOCH)).thenReturn(true);
        when(coordination.isTaskStartedTransaction("1", EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(lifecycle).prepareSnapshot(true);
        verify(lifecycle).onAllTasksStartedTransaction();
        verify(errorHandler, never()).setProducerThrowable(any());
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
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", TABLES));
        // tasks never start their transaction, and keepAlive fails because the DB connection was killed
        when(coordination.isTaskStartedTransaction("0", EPOCH)).thenReturn(false);
        when(coordination.isTaskStartedTransaction("1", EPOCH)).thenReturn(false);
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
        when(coordination.isTaskJoined("0", EPOCH)).thenReturn(false);
        SmartSnapshotLeader leader = new SmartSnapshotLeader(lifecycle, coordination, errorHandler, EPOCH, 2, true,
                10L, 60_000L, 60_000L, () -> {
                });

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
        when(lifecycle.prepareSnapshot(false)).thenReturn(new SmartSnapshotLifecycleManager.SnapshotSetup("snap", "0/16B3748", TABLES));
        when(coordination.isTaskStartedTransaction("0", EPOCH)).thenReturn(true);

        prep(1, false).run();

        verify(lifecycle).prepareSnapshot(false);
        verify(lifecycle).onAllTasksStartedTransaction();
    }

    // The join exists so we never close the coordination facade while the leader thread is still using
    // it. Here the leader thread is blocked in a call that ignores interrupts (like a JDBC call) and only
    // ends when releaseSnapshot runs. The test asserts that when coordination.stop() is finally called,
    // the leader thread has already finished.
    @Test
    public void stopClosesCoordinationOnlyAfterLeaderThreadHasEnded() throws Exception {
        CountDownLatch released = new CountDownLatch(1);
        Thread leader = new Thread(() -> {
            while (released.getCount() > 0) {
                try {
                    released.await();
                }
                catch (InterruptedException e) {
                    // ignore, to model a database call that cannot be interrupted
                }
            }
        }, "leader");
        leader.start();

        SmartSnapshotLifecycleManager lifecycle = mock(SmartSnapshotLifecycleManager.class);
        // releaseSnapshot is what ends the blocked leader thread, standing in for aborting the connection
        doAnswer(inv -> {
            released.countDown();
            return null;
        }).when(lifecycle).releaseSnapshot();

        AtomicBoolean leaderAliveAtCoordinationStop = new AtomicBoolean(true);
        SnapshotCoordinationFacade coordination = mock(SnapshotCoordinationFacade.class);
        doAnswer(inv -> {
            leaderAliveAtCoordinationStop.set(leader.isAlive());
            return null;
        }).when(coordination).stop();

        SmartSnapshotLeader.stopSmartSnapshot(leader, lifecycle, coordination, 2000, "0", 1);

        verify(lifecycle).releaseSnapshot();
        verify(coordination).stop();
        assertThat(leader.isAlive()).isFalse();
        // the key property: coordination was stopped only after the prep thread had finished
        assertThat(leaderAliveAtCoordinationStop.get()).isFalse();
    }
}
