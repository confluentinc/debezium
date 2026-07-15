/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.TaskStateMetrics;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.snapshot.Snapshotter;

public class ChangeEventSourceCoordinatorTest {

    SnapshotterService snapshotterService;
    Snapshotter snapshotter;
    CommonConnectorConfig connectorConfig;
    ChangeEventSourceCoordinator coordinator;
    ChangeEventSource.ChangeEventSourceContext context;

    @Before
    public void before() {
        snapshotterService = mock(SnapshotterService.class);
        snapshotter = mock(Snapshotter.class);
        connectorConfig = mock(CommonConnectorConfig.class);
        when(connectorConfig.getLogicalName()).thenReturn("DummyConnector");
        coordinator = new ChangeEventSourceCoordinator(null, null, SourceConnector.class, connectorConfig, null,
                null, null, null, null, null, snapshotterService);
        context = mock(ChangeEventSource.ChangeEventSourceContext.class);
    }

    @Test
    public void testNotDelayStreamingIfSnapshotShouldNotStream() throws Exception {
        when(snapshotterService.getSnapshotter()).thenReturn(snapshotter);
        when(snapshotter.shouldStream()).thenReturn(false);

        coordinator.delayStreamingIfNeeded(context);

        verify(connectorConfig, never()).getStreamingDelay();
    }

    @Test
    public void testDelayStreamingIfSnapshotShouldStream() throws Exception {
        when(snapshotterService.getSnapshotter()).thenReturn(snapshotter);
        when(snapshotter.shouldStream()).thenReturn(true);
        when(connectorConfig.getStreamingDelay()).thenReturn(Duration.of(1, ChronoUnit.SECONDS));
        when(context.isRunning()).thenReturn(true);

        coordinator.delayStreamingIfNeeded(context);

        verify(connectorConfig, times(1)).getStreamingDelay();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private ChangeEventSourceCoordinator coordinatorForDoSnapshot(TaskStateMetrics taskStateMetrics) {
        EventDispatcher eventDispatcher = mock(EventDispatcher.class);
        DatabaseSchema schema = mock(DatabaseSchema.class);
        ChangeEventSourceCoordinator coordinator = new ChangeEventSourceCoordinator(null, null, SourceConnector.class,
                connectorConfig, null, null, eventDispatcher, schema, null, null, snapshotterService);
        coordinator.taskStateMetrics = taskStateMetrics;
        coordinator.snapshotMetrics = mock(SnapshotChangeEventSourceMetrics.class);
        return coordinator;
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void doSnapshotAssertsThenClearsDnd() throws Exception {
        TaskStateMetrics taskStateMetrics = mock(TaskStateMetrics.class);
        ChangeEventSourceCoordinator coordinator = coordinatorForDoSnapshot(taskStateMetrics);

        SnapshotChangeEventSource snapshotSource = mock(SnapshotChangeEventSource.class);
        SnapshotResult snapshotResult = mock(SnapshotResult.class);
        when(snapshotSource.execute(any(), any(), any(), any())).thenReturn(snapshotResult);

        coordinator.doSnapshot(snapshotSource, context, mock(Partition.class), mock(OffsetContext.class),
                mock(SnapshottingTask.class));

        verify(taskStateMetrics, times(1)).setConnectTaskDnd(1L);
        verify(taskStateMetrics, times(1)).setConnectTaskDnd(0L);
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void doSnapshotClearsDndWhenSnapshotThrows() throws Exception {
        TaskStateMetrics taskStateMetrics = mock(TaskStateMetrics.class);
        ChangeEventSourceCoordinator coordinator = coordinatorForDoSnapshot(taskStateMetrics);

        SnapshotChangeEventSource snapshotSource = mock(SnapshotChangeEventSource.class);
        when(snapshotSource.execute(any(), any(), any(), any())).thenThrow(new RuntimeException("boom"));

        assertThatThrownBy(() -> coordinator.doSnapshot(snapshotSource, context, mock(Partition.class),
                mock(OffsetContext.class), mock(SnapshottingTask.class)))
                .isInstanceOf(RuntimeException.class);

        verify(taskStateMetrics, times(1)).setConnectTaskDnd(1L);
        verify(taskStateMetrics, times(1)).setConnectTaskDnd(0L);
    }

}
