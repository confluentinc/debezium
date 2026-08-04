/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.config.ConfigurationNames;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator.MonitorAction;
import io.debezium.util.Collect;

/**
 * Unit tests for {@link SmartSnapshotConnectorCoordinator}: the config hand-out in {@code taskConfigs}, the
 * epoch/state machine owned by the monitor, the {@code start} decision, and a single monitor iteration.
 */
public class SmartSnapshotConnectorCoordinatorTest {

    @Mock
    private SnapshotCoordinationFacade facade;

    @Mock
    private SourceConnectorContext connectorContext;

    @Mock
    private OffsetStorageReader offsetStorageReader;

    private SmartSnapshotConnectorCoordinator coordinator;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);
        coordinator = new SmartSnapshotConnectorCoordinator(facade, connectorContext, "srv", 30_000L, "connector-context");
    }

    @After
    public void after() {
        coordinator.stop();
    }

    @Test
    public void taskConfigsActiveEmitsOneConfigPerTaskWithEpochAndCount() {
        List<Map<String, String>> configs = coordinator.taskConfigs(2, baseProps());

        assertThat(configs).hasSize(2);
        assertThat(configs.get(0)).containsEntry(ConfigurationNames.TASK_ID_PROPERTY_NAME, "0")
                .containsEntry(SnapshotCoordinationFacade.EPOCH, "1")
                .containsEntry(SnapshotCoordinationFacade.NUM_TASKS, "2");
        assertThat(configs.get(1)).containsEntry(ConfigurationNames.TASK_ID_PROPERTY_NAME, "1");
    }

    @Test
    public void restartBumpsEpochOnNextTaskConfigs() throws Exception {
        coordinator.taskConfigs(2, baseProps()); // sets numTasks = 2, epoch = 1
        when(facade.isRestartNeeded("0", 1)).thenReturn(true);
        // the monitor bumps the epoch; the runtime honors the request by running taskConfigs
        doAnswer(inv -> {
            coordinator.taskConfigs(2, baseProps());
            return null;
        }).when(connectorContext).requestTaskReconfiguration();

        assertThat(coordinator.monitorIteration()).isEqualTo(MonitorAction.CONTINUE_POLLING); // restart requested
        verify(connectorContext).requestTaskReconfiguration();
        verify(facade).writeEpoch(2); // new epoch persisted before the reconfiguration is requested

        List<Map<String, String>> next = coordinator.taskConfigs(2, baseProps());
        assertThat(next.get(0)).containsEntry(SnapshotCoordinationFacade.EPOCH, "2");
    }

    @Test
    public void allTasksDoneCompletesAndWritesCompletion() throws Exception {
        coordinator.taskConfigs(2, baseProps()); // numTasks = 2, epoch = 1
        when(facade.isRestartNeeded(anyString(), eq(1))).thenReturn(false);
        when(facade.isTaskDone("0", 1)).thenReturn(true);
        when(facade.isTaskDone("1", 1)).thenReturn(true);
        when(facade.readSnapshotInfo()).thenReturn(Collect.hashMapOf(SnapshotCoordinationFacade.CONSISTENT_POINT, "0/16B3748"));
        // the monitor writes completion; the runtime honors the request by running taskConfigs
        doAnswer(inv -> {
            coordinator.taskConfigs(2, baseProps());
            return null;
        }).when(connectorContext).requestTaskReconfiguration();

        assertThat(coordinator.monitorIteration()).isEqualTo(MonitorAction.STOP); // downscale requested
        verify(connectorContext).requestTaskReconfiguration();
        assertThat(coordinator.isComplete()).isTrue();
        verify(facade).writeCompletion("0/16B3748", 1);
    }

    @Test
    public void startSkipsWhenAStreamingOffsetAlreadyExists() {
        when(connectorContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        // offset present with no in-progress snapshot marker -> streaming already underway
        when(offsetStorageReader.offset(any())).thenReturn(Collect.hashMapOf("lsn", 123L));

        coordinator.start();

        assertThat(coordinator.isComplete()).isTrue();
        // returns before touching the coordination topic
        verify(facade, never()).start(any());
    }

    @Test
    public void startSkipsWhenCoordinationTopicShowsCompleted() {
        when(connectorContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        when(facade.readSnapshotInfo()).thenReturn(Collect.hashMapOf(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY, true));

        coordinator.start();

        assertThat(coordinator.isComplete()).isTrue();
        verify(facade).start(SnapshotCoordination.MissingTopicPolicy.ASSUME_EXISTS);
    }

    @Test
    public void startFreshReadsAndPersistsTheEpochThenRunsMonitor() {
        when(connectorContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        when(facade.readSnapshotInfo()).thenReturn(null);
        when(facade.readCompletion()).thenReturn(null);
        when(facade.readEpoch()).thenReturn(2);

        coordinator.start();

        assertThat(coordinator.isComplete()).isFalse();
        verify(facade, times(1)).writeEpoch(2); // persistEpoch(readEpoch())
    }

    @Test
    public void startOnVeryFirstDeploymentDefaultsEpochWhenNoneSaved() {
        // no offset, no snapshot info, and no saved epoch (brand-new coordination topic)
        when(connectorContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        when(facade.readSnapshotInfo()).thenReturn(null);
        when(facade.readCompletion()).thenReturn(null);
        when(facade.readEpoch()).thenReturn(null);

        // must not NPE on the null epoch; falls back to the initial epoch (0) and persists it
        coordinator.start();

        assertThat(coordinator.isComplete()).isFalse();
        verify(facade, times(1)).writeEpoch(1);
    }

    // Coordination reads are done outside the state lock, so a slow read on the monitor thread must NOT block
    // taskConfigs on the connector thread.
    @Test
    public void slowCoordinationReadDoesNotBlockTaskConfigs() throws Exception {
        coordinator.taskConfigs(2, baseProps()); // sets lastNumTasks=2, state ACTIVE

        CountDownLatch inRead = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        when(facade.isRestartNeeded(anyString(), anyInt())).thenAnswer(inv -> {
            inRead.countDown();
            proceed.await();
            return false;
        });

        Thread monitor = new Thread(() -> coordinator.monitorIteration(), "monitor");
        monitor.start();
        // monitor is parked in the coordination read, holding no lock
        assertThat(inRead.await(5, TimeUnit.SECONDS)).isTrue();

        // taskConfigs must proceed even though the monitor read is still blocked
        AtomicBoolean taskConfigsReturned = new AtomicBoolean(false);
        Thread cfg = new Thread(() -> {
            coordinator.taskConfigs(2, baseProps());
            taskConfigsReturned.set(true);
        }, "taskConfigs");
        cfg.start();
        cfg.join(2000);
        assertThat(taskConfigsReturned).isTrue();

        proceed.countDown();
        monitor.join(2000);
    }

    // volatile monitorThread + join. A running monitor thread must actually terminate on stop()
    // (no leak), and stop() must see the freshly-published thread reference.
    @Test
    public void stopTerminatesRunningMonitorThread() throws Exception {
        coordinator = new SmartSnapshotConnectorCoordinator(facade, connectorContext, "srv", 10L, "connector-context");
        when(connectorContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        when(facade.readSnapshotInfo()).thenReturn(null);
        when(facade.readCompletion()).thenReturn(null);
        when(facade.readEpoch()).thenReturn(1);
        when(facade.isRestartNeeded(anyString(), anyInt())).thenReturn(false);
        when(facade.isTaskDone(anyString(), anyInt())).thenReturn(false);

        coordinator.start(); // launches the monitor thread
        coordinator.taskConfigs(2, baseProps()); // lastNumTasks=2 so the loop does real work
        Thread monitor = coordinator.monitorThread();
        assertThat(monitor).isNotNull();
        Thread.sleep(100); // let it poll a few times

        coordinator.stop();

        assertThat(monitor.isAlive()).isFalse(); // joined and dead -> no leak
        assertThat(coordinator.monitorThread()).isNull();
    }

    // On a real running thread, one throwing iteration must not kill the
    // monitor — it logs and keeps polling.
    @Test
    public void monitorThreadSurvivesAThrowingIteration() throws Exception {
        coordinator = new SmartSnapshotConnectorCoordinator(facade, connectorContext, "srv", 10L, "connector-context");
        when(connectorContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        when(facade.readCompletion()).thenReturn(null);
        when(facade.readSnapshotInfo()).thenReturn(null);
        when(facade.readEpoch()).thenReturn(1);

        CountDownLatch keptPolling = new CountDownLatch(3); // needs 3 iterations -> proves it survived
        AtomicInteger calls = new AtomicInteger();
        when(facade.isRestartNeeded(anyString(), anyInt())).thenAnswer(inv -> {
            keptPolling.countDown();
            if (calls.getAndIncrement() == 0) {
                throw new RuntimeException("Fetching restart info threw"); // first iteration explodes
            }
            return false;
        });
        when(facade.isTaskDone(anyString(), anyInt())).thenReturn(false);

        coordinator.start();
        coordinator.taskConfigs(2, baseProps());

        assertThat(keptPolling.await(5, TimeUnit.SECONDS)).isTrue(); // survived and kept going
        verify(connectorContext, never()).raiseError(any()); // one bad tick didn't fail the connector
    }

    // If the thread dies from something the loop didn't catch, the
    // connector is failed (so the runtime restarts it) instead of hanging silently.
    @Test
    public void monitorThreadDeathFailsTheConnector() {
        when(connectorContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        when(facade.readSnapshotInfo()).thenReturn(null);
        when(facade.readCompletion()).thenReturn(null);
        when(facade.readEpoch()).thenReturn(1);
        coordinator.start();

        Thread monitor = coordinator.monitorThread();
        assertThat(monitor).isNotNull();
        monitor.getUncaughtExceptionHandler().uncaughtException(monitor, new RuntimeException("fatal"));

        verify(connectorContext).raiseError(any(RuntimeException.class));
    }

    // the monitor thread and the connector thread (taskConfigs)
    // both drive the state machine through restart -> epoch bump -> complete. requestTaskReconfiguration() is
    // wired to run taskConfigs on a SEPARATE thread, exactly like the Connect runtime, so both actors really
    // contend on stateLock. Invariants: exactly one bump to epoch 2, completion written once and only at the
    // final epoch (never the stale epoch 1), and the monitor stops.
    @Test
    public void monitorAndTaskConfigsRaceThroughRestartBumpAndComplete() throws Exception {
        coordinator = new SmartSnapshotConnectorCoordinator(facade, connectorContext, "srv", 10L, "connector-context");

        // start() preconditions: fresh snapshot, saved epoch = 1
        when(connectorContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        when(facade.readSnapshotInfo()).thenReturn(Collect.hashMapOf(SnapshotCoordinationFacade.CONSISTENT_POINT,
                "0/16B3748"));
        when(facade.readCompletion()).thenReturn(null);
        when(facade.readEpoch()).thenReturn(1);

        // Coordination state the two threads observe:
        // epoch 1 -> a task needs a restart, nobody is done
        // epoch 2 -> no restart, everybody is done
        when(facade.isRestartNeeded(anyString(), eq(1))).thenReturn(true);
        when(facade.isRestartNeeded(anyString(), eq(2))).thenReturn(false);
        when(facade.isTaskDone(anyString(), eq(1))).thenReturn(false);
        when(facade.isTaskDone(anyString(), eq(2))).thenReturn(true);

        // fires when the connector thread writes completion for the final epoch
        CountDownLatch completed = new CountDownLatch(1);
        doAnswer(inv -> {
            completed.countDown();
            return null;
        }).when(facade).writeCompletion(anyString(), eq(2));

        // requestTaskReconfiguration() drives taskConfigs on a DIFFERENT thread, like the Connect runtime.
        ExecutorService connectorThread = Executors.newSingleThreadExecutor(r -> new Thread(r, "connector"));
        doAnswer(inv -> {
            connectorThread.submit(() -> coordinator.taskConfigs(2, baseProps()));
            return null;
        }).when(connectorContext).requestTaskReconfiguration();

        try {
            coordinator.start(); // monitor thread starts (no-ops until lastNumTasks is set)
            coordinator.taskConfigs(2, baseProps()); // initial hand-out: epoch 1, lastNumTasks = 2

            // the whole restart -> bump -> complete cycle runs across the two threads
            assertThat(completed.await(5, TimeUnit.SECONDS)).isTrue();

            // invariants after the race settles:
            assertThat(coordinator.isComplete()).isTrue();
            verify(facade).writeEpoch(2); // bumped exactly once, 1 -> 2
            verify(facade).writeCompletion("0/16B3748", 2); // completed at the FINAL epoch
            verify(facade, never()).writeCompletion(anyString(), eq(1)); // never at the stale epoch
            verify(connectorContext, atLeast(2)).requestTaskReconfiguration(); // restart + complete

            // downscale is idempotent: a taskConfigs after completion still returns the single streaming config
            assertThat(coordinator.taskConfigs(2, baseProps())).hasSize(1);
        }
        finally {
            connectorThread.shutdownNow();
        }
    }

    // If a restart reconfiguration fails to submit, the monitor fails the connector so the runtime restarts it.
    // The new epoch is already durable, so the restart resumes from it.
    @Test
    public void restartReconfigurationSubmitFailureFailsConnector() throws Exception {
        coordinator.taskConfigs(2, baseProps()); // epoch = 1, numTasks = 2, state ACTIVE
        when(facade.isRestartNeeded("0", 1)).thenReturn(true);
        doThrow(new RuntimeException("kafka down")).when(connectorContext).requestTaskReconfiguration();

        assertThat(coordinator.monitorIteration()).isEqualTo(MonitorAction.STOP); // submit failed -> connector failed
        verify(connectorContext).requestTaskReconfiguration();
        verify(facade).writeEpoch(2); // new epoch persisted before the (failed) request
        verify(connectorContext).raiseError(any(RuntimeException.class));
    }

    // Same on the completion path: if the downscale reconfiguration fails to submit, the monitor fails the
    // connector. The completion marker is already durable, so the restart skips the snapshot and downscales.
    @Test
    public void completionReconfigurationSubmitFailureFailsConnector() throws Exception {
        coordinator.taskConfigs(2, baseProps()); // epoch = 1, numTasks = 2
        when(facade.isRestartNeeded(anyString(), eq(1))).thenReturn(false);
        when(facade.isTaskDone("0", 1)).thenReturn(true);
        when(facade.isTaskDone("1", 1)).thenReturn(true);
        when(facade.readSnapshotInfo()).thenReturn(Collect.hashMapOf(SnapshotCoordinationFacade.CONSISTENT_POINT, "0/16B3748"));
        doThrow(new RuntimeException("kafka down")).when(connectorContext).requestTaskReconfiguration();

        assertThat(coordinator.monitorIteration()).isEqualTo(MonitorAction.STOP); // submit failed -> connector failed
        assertThat(coordinator.isComplete()).isTrue();
        verify(facade).writeCompletion("0/16B3748", 1); // marker persisted before the (failed) request
        verify(connectorContext).raiseError(any(RuntimeException.class));
    }

    // The monitor fires the reconfiguration and moves on; it does not wait for the runtime to honor it. A request
    // the runtime never acts on must therefore neither block the iteration nor fail the connector — it is recovered
    // on the next connector restart, when taskConfigs() hands out configs stamped with the new epoch.
    @Test
    public void monitorDoesNotWaitForTheRuntimeToHonorTheRequest() {
        coordinator.taskConfigs(2, baseProps()); // epoch = 1, numTasks = 2
        when(facade.isRestartNeeded("0", 1)).thenReturn(true);
        // requestTaskReconfiguration is a plain no-op mock: the runtime never calls taskConfigs back

        assertThat(coordinator.monitorIteration()).isEqualTo(MonitorAction.CONTINUE_POLLING); // returned without waiting
        verify(connectorContext).requestTaskReconfiguration();
        verify(facade).writeEpoch(2); // new epoch already durable, so a dropped request is recoverable
        verify(connectorContext, never()).raiseError(any());
    }

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put("connector.class", "x");
        return props;
    }
}
