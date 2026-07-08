/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.PostgresConnectorTask.SmartSnapshotLeader;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager;
import io.debezium.pipeline.source.snapshot.SmartSnapshotLifecycleManager.SnapshotSetup;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.relational.TableId;

/**
 * Unit tests for the leader (task-0) snapshot-preparation orchestration extracted from
 * {@code PostgresConnectorTask}. A mock lifecycle + coordination let us assert the sequence without a
 * database or Kafka. pollMs = 0 so the wait loop does not sleep.
 */
public class PostgresConnectorTaskLeaderPreparationTest {

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
        return new SmartSnapshotLeader(lifecycle, coordination, errorHandler, EPOCH, numTasks, shouldStream,
                0L, () -> {
                });
    }

    @Test
    public void skipsPreparationWhenLeaderAlreadyCompleted() {
        when(coordination.isDone("0", EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(coordination).start();
        verify(lifecycle, never()).prepareSnapshot(anyBoolean());
        verify(coordination, never()).writeSnapshotInfo(any(), any(), eq(EPOCH), any(), eq(2));
    }

    @Test
    public void skipsPreparationWhenRestartSignalled() {
        when(coordination.isRestartNeeded("1", EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(coordination).start();
        verify(lifecycle, never()).prepareSnapshot(anyBoolean());
        verify(coordination, never()).writeSnapshotInfo(any(), any(), eq(EPOCH), any(), eq(2));
    }

    @Test
    public void preparesPublishesAndReleasesWhenAllTasksJoin() {
        when(coordination.isDone("0", EPOCH)).thenReturn(false);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SnapshotSetup("snap", "0/16B3748", TABLES));
        when(coordination.isTransactionStarted("0", EPOCH)).thenReturn(true);
        when(coordination.isTransactionStarted("1", EPOCH)).thenReturn(true);

        prep(2, true).run();

        verify(lifecycle).prepareSnapshot(true);
        verify(coordination).writeSnapshotInfo("snap", "0/16B3748", EPOCH, TABLES, 2);
        verify(lifecycle).onAllTasksStartedTransaction();
        verify(lifecycle, never()).releaseSnapshot();
    }

    @Test
    public void keepsSnapshotAliveWhileWaitingForTasks() {
        when(coordination.isDone("0", EPOCH)).thenReturn(false);
        when(lifecycle.prepareSnapshot(true)).thenReturn(new SnapshotSetup("snap", "0/16B3748", TABLES));
        // not joined on the first check, joined afterwards -> the wait loop runs one iteration
        when(coordination.isTransactionStarted("0", EPOCH)).thenReturn(false, true);

        prep(1, true).run();

        verify(lifecycle, times(1)).keepAlive();
        verify(lifecycle).onAllTasksStartedTransaction();
    }

    @Test
    public void onPreparationFailureReleasesAndFailsTheTask() {
        when(coordination.isDone("0", EPOCH)).thenReturn(false);
        when(lifecycle.prepareSnapshot(anyBoolean())).thenThrow(new RuntimeException("boom"));

        prep(2, true).run();

        verify(lifecycle).releaseSnapshot();
        verify(errorHandler).setProducerThrowable(any(DebeziumException.class));
        verify(lifecycle, never()).onAllTasksStartedTransaction();
    }

    @Test
    public void nonStreamingModePreparesWithoutSlot() {
        when(coordination.isDone("0", EPOCH)).thenReturn(false);
        when(lifecycle.prepareSnapshot(false)).thenReturn(new SnapshotSetup("snap", "0/16B3748", TABLES));
        when(coordination.isTransactionStarted("0", EPOCH)).thenReturn(true);

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

        PostgresConnectorTask.stopSmartSnapshot(leader, lifecycle, coordination, 2000, 1);

        verify(lifecycle).releaseSnapshot();
        verify(coordination).stop();
        assertThat(leader.isAlive()).isFalse();
        // the key property: coordination was stopped only after the prep thread had finished
        assertThat(leaderAliveAtCoordinationStop.get()).isFalse();
    }
}
