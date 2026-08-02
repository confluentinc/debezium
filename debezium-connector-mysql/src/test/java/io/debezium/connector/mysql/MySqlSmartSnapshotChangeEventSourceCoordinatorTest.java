/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Collect;
import io.debezium.util.LoggingContext;

/**
 * Unit tests for the per-task decision ladder in
 * {@code MySqlSmartSnapshotChangeEventSourceCoordinator#executeChangeEventSources}. The ladder itself lives in
 * {@code AbstractSmartSnapshotChangeEventSourceCoordinator}; a test subclass stubs the inherited {@code doSnapshot}
 * so the branches (already-done / rejoin / stale-epoch / fresh-join / snapshot-failure) can be exercised without a
 * database or a real snapshot.
 *
 * <p>MySQL differs from Postgres in the last-mile wiring the coordinator does: the consistent point is published as
 * {@code file:pos:gtids} (not an LSN) and the table assignment is catalog-scoped, so {@code configureSmartSource}
 * decodes those and calls {@link MySqlSmartSnapshotChangeEventSource#setSmartSnapshot} rather than Postgres's
 * {@code setSnapshotCoordination}. The fresh-join test asserts that wiring.
 */
public class MySqlSmartSnapshotChangeEventSourceCoordinatorTest {

    private static final String TASK_ID = "1";
    private static final int EPOCH = 4;
    // P is published as "file:pos:gtids"; the gtids segment itself contains ':' (uuid:range), which the
    // coordinator must keep together via a 3-way split.
    private static final String CONSISTENT_POINT = "mysql-bin.000003:456:123e4567:1-10";

    @Mock
    private ErrorHandler errorHandler;
    @Mock
    private MySqlChangeEventSourceFactory changeEventSourceFactory;
    @Mock
    private ChangeEventSourceMetricsFactory<MySqlPartition> metricsFactory;
    @Mock
    private EventDispatcher<MySqlPartition, TableId> eventDispatcher;
    @Mock
    private DatabaseSchema<?> schema;
    @Mock
    private SnapshotterService snapshotterService;
    @Mock
    private SignalProcessor<MySqlPartition, MySqlOffsetContext> signalProcessor;
    @Mock
    private NotificationService<MySqlPartition, MySqlOffsetContext> notificationService;
    @Mock
    private SnapshotCoordinationFacade coordination;
    @Mock
    private Offsets<MySqlPartition, MySqlOffsetContext> previousOffsets;
    @Mock
    private MySqlTaskContext taskContext;
    @Mock
    private ChangeEventSourceContext context;
    @Mock
    private MySqlSmartSnapshotChangeEventSource snapshotSource;
    @Mock
    private MySqlPartition partition;

    private TestCoordinator coordinator;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);

        Configuration config = Configuration.create()
                .with(BinlogConnectorConfig.HOSTNAME, "localhost")
                .with(BinlogConnectorConfig.USER, "user")
                .with(BinlogConnectorConfig.PASSWORD, "pass")
                // satisfies the JDBC credentials provider during config construction
                .with("jdbc.creds.provider.user", "user")
                .with("jdbc.creds.provider.password", "pass")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "test_server")
                .with(ConfigurationNames.TASK_ID_PROPERTY_NAME, TASK_ID)
                .build();
        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);

        when(previousOffsets.getTheOnlyPartition()).thenReturn(partition);
        when(taskContext.configureLoggingContext(anyString(), any())).thenReturn(mockLogContext());

        coordinator = new TestCoordinator(previousOffsets, errorHandler, connectorConfig, changeEventSourceFactory,
                metricsFactory, eventDispatcher, schema, snapshotterService, signalProcessor, notificationService,
                EPOCH, coordination, TASK_ID);
        // Don't sleep the default 10s poll interval when a test drives the snapshot-info wait loop.
        coordinator.setSnapshotInfoPollIntervalMs(0);
    }

    private void execute() throws InterruptedException {
        coordinator.executeChangeEventSources(taskContext, snapshotSource, previousOffsets,
                new AtomicReference<>(), context);
    }

    // Published snapshot info with a MySQL-style catalog-scoped assignment for this task.
    private static java.util.Map<String, Object> snapshotInfo() {
        return Collect.hashMapOf(
                SnapshotCoordinationFacade.SNAPSHOT_NAME, null,
                SnapshotCoordinationFacade.EPOCH, EPOCH,
                SnapshotCoordinationFacade.CONSISTENT_POINT, CONSISTENT_POINT,
                SnapshotCoordinationFacade.ASSIGNMENTS, java.util.Map.of(TASK_ID, java.util.List.of("\"db\".\"a\"")),
                SnapshotCoordinationFacade.NUM_TASKS, 2);
    }

    @Test
    public void alreadyDoneTaskIdlesWithoutSnapshotting() throws Exception {
        when(coordination.isTaskDone(TASK_ID, EPOCH)).thenReturn(true);

        execute();

        assertThat(coordinator.doSnapshotCalled).isFalse();
        verify(coordination, never()).writeTaskJoin(anyString(), anyInt());
    }

    @Test
    public void rejoinAfterTransactionStartSignalsRestartWithoutSnapshotting() throws Exception {
        when(coordination.isTaskDone(TASK_ID, EPOCH)).thenReturn(false);
        // the task had already started its transaction this epoch -> it may be mid-slice, so a clean round is needed
        when(coordination.isTaskStartedTransaction(TASK_ID, EPOCH)).thenReturn(true);

        execute();

        verify(coordination).writeRestartNeeded(TASK_ID, EPOCH);
        assertThat(coordinator.doSnapshotCalled).isFalse();
    }

    @Test
    public void joinedButNotStartedTransactionReRunsWithoutRestart() throws Exception {
        when(coordination.isTaskDone(TASK_ID, EPOCH)).thenReturn(false);
        // a join marker for this epoch is present, but the task never started its transaction (e.g. it died while
        // waiting for the snapshot). This must NOT force a restart; the task re-runs cleanly at the same epoch.
        when(coordination.readTaskJoinEpoch(TASK_ID)).thenReturn(EPOCH);
        when(coordination.isTaskStartedTransaction(TASK_ID, EPOCH)).thenReturn(false);
        when(coordination.readEpoch()).thenReturn(null);
        when(coordination.readSnapshotInfo()).thenReturn(snapshotInfo());

        execute();

        verify(coordination, never()).writeRestartNeeded(anyString(), anyInt());
        assertThat(coordinator.doSnapshotCalled).isTrue();
        verify(coordination).writeTaskDone(TASK_ID, EPOCH);
    }

    @Test
    public void staleEpochIdlesWithoutSnapshotting() throws Exception {
        when(coordination.isTaskDone(TASK_ID, EPOCH)).thenReturn(false);
        when(coordination.readTaskJoinEpoch(TASK_ID)).thenReturn(null);
        when(coordination.readEpoch()).thenReturn(EPOCH + 1); // connector already advanced

        execute();

        verify(coordination, never()).writeTaskJoin(anyString(), anyInt());
        assertThat(coordinator.doSnapshotCalled).isFalse();
    }

    @Test
    public void freshJoinWritesMarkerSnapshotsThenCompletes() throws Exception {
        when(coordination.isTaskDone(TASK_ID, EPOCH)).thenReturn(false);
        when(coordination.readTaskJoinEpoch(TASK_ID)).thenReturn(null);
        when(coordination.readEpoch()).thenReturn(null);
        when(coordination.readSnapshotInfo()).thenReturn(snapshotInfo());

        execute();

        assertThat(coordinator.doSnapshotCalled).isTrue();
        InOrder order = inOrder(coordination);
        order.verify(coordination).writeTaskJoin(TASK_ID, EPOCH);
        order.verify(coordination).readSnapshotInfo();
        order.verify(coordination).writeTaskDone(TASK_ID, EPOCH);
        // MySQL wiring: decode "file:pos:gtids" into (file, pos, gtid) and hand this task's catalog-scoped slice to
        // the smart source. The gtids segment (itself containing ':') must survive the 3-way split intact.
        verify(snapshotSource).setSmartSnapshot(eq(EPOCH), eq("mysql-bin.000003"), eq(456L), eq("123e4567:1-10"),
                any(), eq(coordination));
    }

    @Test
    public void transientReadFailureWhileWaitingForSnapshotInfoIsTolerated() throws Exception {
        when(coordination.isTaskDone(TASK_ID, EPOCH)).thenReturn(false);
        when(coordination.readTaskJoinEpoch(TASK_ID)).thenReturn(null);
        when(coordination.readEpoch()).thenReturn(null);
        // the first snapshot-info read blips (broker hiccup); the poll loop must keep polling and pick up the
        // snapshot on a later attempt, not fail the task on a single transient read failure.
        when(coordination.readSnapshotInfo())
                .thenThrow(new DebeziumException("read blip"))
                .thenReturn(snapshotInfo());

        execute();

        assertThat(coordinator.doSnapshotCalled).isTrue();
        verify(coordination).writeTaskDone(TASK_ID, EPOCH);
        verify(coordination, never()).writeRestartNeeded(anyString(), anyInt());
    }

    @Test
    public void snapshotFailureSignalsRestartAndRethrows() {
        when(coordination.isTaskDone(TASK_ID, EPOCH)).thenReturn(false);
        when(coordination.readTaskJoinEpoch(TASK_ID)).thenReturn(null);
        when(coordination.readEpoch()).thenReturn(null);
        when(coordination.readSnapshotInfo()).thenReturn(snapshotInfo());
        coordinator.snapshotError = new RuntimeException("snapshot read failed");

        assertThatThrownBy(this::execute).isInstanceOf(RuntimeException.class);

        verify(coordination).writeRestartNeeded(TASK_ID, EPOCH);
    }

    @Test
    public void interruptDuringSnapshotDoesNotWriteCompletion() throws Exception {
        when(coordination.isTaskDone(TASK_ID, EPOCH)).thenReturn(false);
        when(coordination.readTaskJoinEpoch(TASK_ID)).thenReturn(null);
        when(coordination.readEpoch()).thenReturn(null);
        when(coordination.readSnapshotInfo()).thenReturn(snapshotInfo());
        coordinator.interruptDuringSnapshot = true;

        try {
            execute();

            assertThat(coordinator.doSnapshotCalled).isTrue();
            // An interrupt means the snapshot did not finish: the subset must NOT be marked done, otherwise the
            // monitor could downscale it and isTaskDone() would skip the snapshot on the next restart.
            verify(coordination, never()).writeTaskDone(anyString(), anyInt());
            // The task is stopping, not crashing mid-snapshot, so no restart is signalled either.
            verify(coordination, never()).writeRestartNeeded(anyString(), anyInt());
            // The interrupt status is preserved so the caller can shut down cleanly.
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        }
        finally {
            // Clear the interrupt flag so it does not leak into other tests on this thread.
            Thread.interrupted();
        }
    }

    @Test
    public void wrappedInterruptDuringSnapshotDoesNotSignalRestart() throws Exception {
        when(coordination.isTaskDone(TASK_ID, EPOCH)).thenReturn(false);
        when(coordination.readTaskJoinEpoch(TASK_ID)).thenReturn(null);
        when(coordination.readEpoch()).thenReturn(null);
        when(coordination.readSnapshotInfo()).thenReturn(snapshotInfo());
        // Interrupt that surfaces as a wrapped exception (e.g. a JDBC/producer call) rather than
        // InterruptedException: the interrupt flag is set, but the throw is a plain exception.
        coordinator.snapshotError = new RuntimeException("read interrupted");
        coordinator.setInterruptFlagBeforeError = true;

        try {
            // Should exit gracefully, not rethrow, because the interrupt flag is set.
            execute();

            assertThat(coordinator.doSnapshotCalled).isTrue();
            // On the next start the rejoin path handles cleanup: if this task had started its transaction it
            // signals restart, otherwise it re-runs cleanly. So we neither write restart_needed nor mark done here.
            verify(coordination, never()).writeRestartNeeded(anyString(), anyInt());
            verify(coordination, never()).writeTaskDone(anyString(), anyInt());
        }
        finally {
            // Clear the interrupt flag so it does not leak into other tests on this thread.
            Thread.interrupted();
        }
    }

    private static LoggingContext.PreviousContext mockLogContext() {
        return org.mockito.Mockito.mock(LoggingContext.PreviousContext.class);
    }

    /**
     * Overrides the inherited {@code doSnapshot} so the branch logic can be tested without running a real
     * snapshot; records whether it was invoked and optionally throws to simulate a snapshot failure.
     */
    static class TestCoordinator extends MySqlSmartSnapshotChangeEventSourceCoordinator {

        boolean doSnapshotCalled;
        RuntimeException snapshotError;
        boolean interruptDuringSnapshot;
        boolean setInterruptFlagBeforeError;

        TestCoordinator(Offsets<MySqlPartition, MySqlOffsetContext> previousOffsets, ErrorHandler errorHandler,
                        MySqlConnectorConfig connectorConfig, MySqlChangeEventSourceFactory changeEventSourceFactory,
                        ChangeEventSourceMetricsFactory<MySqlPartition> metricsFactory,
                        EventDispatcher<MySqlPartition, ?> eventDispatcher, DatabaseSchema<?> schema,
                        SnapshotterService snapshotterService, SignalProcessor<MySqlPartition, MySqlOffsetContext> signalProcessor,
                        NotificationService<MySqlPartition, MySqlOffsetContext> notificationService, int epoch,
                        SnapshotCoordinationFacade coordination, String taskId) {
            super(previousOffsets, errorHandler, MySqlConnector.class, connectorConfig, changeEventSourceFactory,
                    metricsFactory, eventDispatcher, schema, snapshotterService, signalProcessor,
                    notificationService, epoch, coordination, taskId);
        }

        // widen visibility so the test (same package, not a subclass) can shorten the poll interval
        @Override
        public void setSnapshotInfoPollIntervalMs(long ms) {
            super.setSnapshotInfoPollIntervalMs(ms);
        }

        // widen visibility so the test can drive the orchestration (the method is protected on the core base)
        @Override
        public void executeChangeEventSources(CdcSourceTaskContext taskContext,
                                              SnapshotChangeEventSource<MySqlPartition, MySqlOffsetContext> snapshotSource,
                                              Offsets<MySqlPartition, MySqlOffsetContext> previousOffsets,
                                              AtomicReference<LoggingContext.PreviousContext> previousLogContext,
                                              ChangeEventSourceContext context)
                throws InterruptedException {
            super.executeChangeEventSources(taskContext, snapshotSource, previousOffsets, previousLogContext, context);
        }

        @Override
        protected SnapshotResult<MySqlOffsetContext> doSnapshot(
                                                                SnapshotChangeEventSource<MySqlPartition, MySqlOffsetContext> snapshotSource,
                                                                ChangeEventSourceContext context, MySqlPartition partition,
                                                                MySqlOffsetContext previousOffset)
                throws InterruptedException {
            doSnapshotCalled = true;
            if (interruptDuringSnapshot) {
                throw new InterruptedException("snapshot interrupted");
            }
            if (snapshotError != null) {
                // Simulate an interrupt that surfaces as a wrapped exception rather than InterruptedException:
                // the interrupt flag is set but a plain exception is thrown from the snapshot call.
                if (setInterruptFlagBeforeError) {
                    Thread.currentThread().interrupt();
                }
                throw snapshotError;
            }
            return SnapshotResult.completed(null);
        }
    }
}
