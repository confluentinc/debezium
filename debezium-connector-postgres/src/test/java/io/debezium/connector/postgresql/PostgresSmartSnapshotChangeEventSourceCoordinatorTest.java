/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

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

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
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
 * {@link PostgresSmartSnapshotChangeEventSourceCoordinator#executeChangeEventSources}. A test subclass stubs
 * the inherited {@code doSnapshot} so the branches (already-done / rejoin / stale-epoch / fresh-join /
 * snapshot-failure) can be exercised without a database or a real snapshot.
 */
public class PostgresSmartSnapshotChangeEventSourceCoordinatorTest {

    private static final String TASK_ID = "1";
    private static final int EPOCH = 4;

    @Mock
    private ErrorHandler errorHandler;
    @Mock
    private PostgresChangeEventSourceFactory changeEventSourceFactory;
    @Mock
    private ChangeEventSourceMetricsFactory<PostgresPartition> metricsFactory;
    @Mock
    private EventDispatcher<PostgresPartition, TableId> eventDispatcher;
    @Mock
    private DatabaseSchema<?> schema;
    @Mock
    private SnapshotterService snapshotterService;
    @Mock
    private SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor;
    @Mock
    private NotificationService<PostgresPartition, PostgresOffsetContext> notificationService;
    @Mock
    private SnapshotCoordinationFacade coordination;
    @Mock
    private Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets;
    @Mock
    private PostgresTaskContext taskContext;
    @Mock
    private ChangeEventSourceContext context;
    @Mock
    private PostgresSmartSnapshotChangeEventSource snapshotSource;
    @Mock
    private PostgresPartition partition;

    private TestCoordinator coordinator;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);

        Configuration config = Configuration.create()
                .with(PostgresConnectorConfig.HOSTNAME, "localhost")
                .with(PostgresConnectorConfig.PORT, 5432)
                .with(PostgresConnectorConfig.USER, "user")
                .with(PostgresConnectorConfig.PASSWORD, "pass")
                .with(PostgresConnectorConfig.DATABASE_NAME, "db")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "test_server")
                .with(ConfigurationNames.TASK_ID_PROPERTY_NAME, TASK_ID)
                .build();
        PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(config);

        when(previousOffsets.getTheOnlyPartition()).thenReturn(partition);
        when(taskContext.configureLoggingContext(anyString(), any())).thenReturn(mockLogContext());
        when(context.isRunning()).thenReturn(false); // idleUntilRestart returns immediately

        coordinator = new TestCoordinator(previousOffsets, errorHandler, connectorConfig, changeEventSourceFactory,
                metricsFactory, eventDispatcher, schema, snapshotterService, signalProcessor, notificationService,
                EPOCH, coordination, TASK_ID);
    }

    private void execute() throws InterruptedException {
        coordinator.executeChangeEventSources(taskContext, snapshotSource, previousOffsets,
                new AtomicReference<>(), context);
    }

    @Test
    public void alreadyDoneTaskIdlesWithoutSnapshotting() throws Exception {
        when(coordination.isDone(TASK_ID, EPOCH)).thenReturn(true);

        execute();

        assertThat(coordinator.doSnapshotCalled).isFalse();
        verify(coordination, never()).writeJoin(anyString(), anyInt());
    }

    @Test
    public void rejoinSignalsRestartWithoutSnapshotting() throws Exception {
        when(coordination.isDone(TASK_ID, EPOCH)).thenReturn(false);
        when(coordination.readJoinEpoch(TASK_ID)).thenReturn(EPOCH); // marker for this epoch already present

        execute();

        verify(coordination).writeRestartNeeded(TASK_ID, EPOCH);
        assertThat(coordinator.doSnapshotCalled).isFalse();
    }

    @Test
    public void staleEpochIdlesWithoutSnapshotting() throws Exception {
        when(coordination.isDone(TASK_ID, EPOCH)).thenReturn(false);
        when(coordination.readJoinEpoch(TASK_ID)).thenReturn(null);
        when(coordination.readEpoch()).thenReturn(EPOCH + 1); // connector already advanced

        execute();

        verify(coordination, never()).writeJoin(anyString(), anyInt());
        assertThat(coordinator.doSnapshotCalled).isFalse();
    }

    @Test
    public void freshJoinWritesMarkerSnapshotsThenCompletes() throws Exception {
        when(coordination.isDone(TASK_ID, EPOCH)).thenReturn(false);
        when(coordination.readJoinEpoch(TASK_ID)).thenReturn(null);
        when(coordination.readEpoch()).thenReturn(null);
        when(coordination.readSnapshotInfo()).thenReturn(Collect.hashMapOf(
                SnapshotCoordinationFacade.SNAPSHOT_NAME, "snap",
                SnapshotCoordinationFacade.EPOCH, EPOCH,
                SnapshotCoordinationFacade.CONSISTENT_POINT, "0/16B3748",
                SnapshotCoordinationFacade.TABLES, "public.a",
                SnapshotCoordinationFacade.NUM_TASKS, 2));

        execute();

        assertThat(coordinator.doSnapshotCalled).isTrue();
        InOrder order = inOrder(coordination);
        order.verify(coordination).writeJoin(TASK_ID, EPOCH);
        order.verify(coordination).readSnapshotInfo();
        order.verify(coordination).writeDone(TASK_ID, EPOCH);
        verify(snapshotSource).setSnapshotCoordination(eq(EPOCH), eq("snap"), any(), any(), eq(coordination));
    }

    @Test
    public void snapshotFailureSignalsRestartAndRethrows() {
        when(coordination.isDone(TASK_ID, EPOCH)).thenReturn(false);
        when(coordination.readJoinEpoch(TASK_ID)).thenReturn(null);
        when(coordination.readEpoch()).thenReturn(null);
        when(coordination.readSnapshotInfo()).thenReturn(Collect.hashMapOf(
                SnapshotCoordinationFacade.SNAPSHOT_NAME, "snap",
                SnapshotCoordinationFacade.EPOCH, EPOCH,
                SnapshotCoordinationFacade.CONSISTENT_POINT, "0/16B3748",
                SnapshotCoordinationFacade.TABLES, "public.a",
                SnapshotCoordinationFacade.NUM_TASKS, 2));
        coordinator.snapshotError = new RuntimeException("snapshot read failed");

        assertThatThrownBy(this::execute).isInstanceOf(RuntimeException.class);

        verify(coordination).writeRestartNeeded(TASK_ID, EPOCH);
    }

    private static LoggingContext.PreviousContext mockLogContext() {
        return org.mockito.Mockito.mock(LoggingContext.PreviousContext.class);
    }

    /**
     * Overrides the inherited {@code doSnapshot} so the branch logic can be tested without running a real
     * snapshot; records whether it was invoked and optionally throws to simulate a snapshot failure.
     */
    static class TestCoordinator extends PostgresSmartSnapshotChangeEventSourceCoordinator {

        boolean doSnapshotCalled;
        RuntimeException snapshotError;

        TestCoordinator(Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets, ErrorHandler errorHandler,
                        PostgresConnectorConfig connectorConfig, PostgresChangeEventSourceFactory changeEventSourceFactory,
                        ChangeEventSourceMetricsFactory<PostgresPartition> metricsFactory,
                        EventDispatcher<PostgresPartition, ?> eventDispatcher, DatabaseSchema<?> schema,
                        SnapshotterService snapshotterService, SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor,
                        NotificationService<PostgresPartition, PostgresOffsetContext> notificationService, int epoch,
                        SnapshotCoordinationFacade coordination, String taskId) {
            super(previousOffsets, errorHandler, PostgresConnector.class, connectorConfig, changeEventSourceFactory,
                    metricsFactory, eventDispatcher, schema, snapshotterService, null, signalProcessor,
                    notificationService, epoch, coordination, taskId);
        }

        @Override
        protected SnapshotResult<PostgresOffsetContext> doSnapshot(
                                                                   SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> snapshotSource,
                                                                   ChangeEventSourceContext context, PostgresPartition partition,
                                                                   PostgresOffsetContext previousOffset)
                throws InterruptedException {
            doSnapshotCalled = true;
            if (snapshotError != null) {
                throw snapshotError;
            }
            return SnapshotResult.completed(null);
        }
    }
}
