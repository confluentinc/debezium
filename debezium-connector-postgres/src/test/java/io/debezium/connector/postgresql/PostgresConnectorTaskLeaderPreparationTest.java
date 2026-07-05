/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.PostgresConnectorTask.LeaderSnapshotPreparation;
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

    private LeaderSnapshotPreparation prep(int numTasks, boolean shouldStream) {
        return new LeaderSnapshotPreparation(lifecycle, coordination, errorHandler, EPOCH, numTasks, shouldStream,
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
        verify(lifecycle).onAllTasksJoined();
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
        verify(lifecycle).onAllTasksJoined();
    }

    @Test
    public void onPreparationFailureReleasesAndFailsTheTask() {
        when(coordination.isDone("0", EPOCH)).thenReturn(false);
        when(lifecycle.prepareSnapshot(anyBoolean())).thenThrow(new RuntimeException("boom"));

        prep(2, true).run();

        verify(lifecycle).releaseSnapshot();
        verify(errorHandler).setProducerThrowable(any(DebeziumException.class));
        verify(lifecycle, never()).onAllTasksJoined();
    }

    @Test
    public void nonStreamingModePreparesWithoutSlot() {
        when(coordination.isDone("0", EPOCH)).thenReturn(false);
        when(lifecycle.prepareSnapshot(false)).thenReturn(new SnapshotSetup("snap", "0/16B3748", TABLES));
        when(coordination.isTransactionStarted("0", EPOCH)).thenReturn(true);

        prep(1, false).run();

        verify(lifecycle).prepareSnapshot(false);
        verify(lifecycle).onAllTasksJoined();
    }
}
