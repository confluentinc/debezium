/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.LoggingContext;

/**
 * The task-side coordinator for a smart snapshot data task. This is the DB-agnostic orchestration:
 * write the join marker, wait for the leader to publish the snapshot info on the coordination topic,
 * run the snapshot of this task's slice, then record completion and idle until the connector downscales.
 *
 * <p>Everything that differs per connector is confined to two hooks: {@link #epochOf(OffsetContext)}
 * reads the epoch stamped on a saved offset, and
 * {@link #configureSmartSource(SnapshotChangeEventSource, int, String, String, Long, Object, SnapshotCoordinationFacade)}
 * hands the published snapshot info to the connector's smart snapshot source (which knows how to decode the
 * consistent position and parse the table assignment for its own identifier style).
 *
 * <p>Catch-up streaming is always off here: a smart snapshot task only takes a data snapshot of its slice
 * and never streams. The base {@link ChangeEventSourceCoordinator#executeCatchUpStreaming} already returns
 * {@code false}, so this class does not override it.
 */
public abstract class AbstractSmartSnapshotChangeEventSourceCoordinator<P extends Partition, O extends OffsetContext>
        extends ChangeEventSourceCoordinator<P, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSmartSnapshotChangeEventSourceCoordinator.class);

    protected final int epoch;
    // Used only to create the task-side coordination facade (see createCoordination()).
    private final Configuration config;
    // This coordinator owns the task-side facade end to end: created, started, used and stopped inside
    // executeChangeEventSources(), all on the executor thread. Set there and only used on that thread.
    private SnapshotCoordinationFacade snapshotCoordination;
    protected final String taskId;
    // How long to wait for the leader to publish the snapshot info before failing this task.
    private final long snapshotInfoWaitTimeoutMs;
    // Interval between snapshot-info poll attempts. Visible for testing so a unit test does not sleep the default.
    private long snapshotInfoPollIntervalMs;

    protected AbstractSmartSnapshotChangeEventSourceCoordinator(
                                                                Offsets<P, O> previousOffsets,
                                                                ErrorHandler errorHandler,
                                                                Class<? extends SourceConnector> connectorType,
                                                                CommonConnectorConfig connectorConfig,
                                                                ChangeEventSourceFactory<P, O> changeEventSourceFactory,
                                                                ChangeEventSourceMetricsFactory<P> changeEventSourceMetricsFactory,
                                                                EventDispatcher<P, ?> eventDispatcher,
                                                                DatabaseSchema<?> schema,
                                                                SignalProcessor<P, O> signalProcessor,
                                                                NotificationService<P, O> notificationService,
                                                                SnapshotterService snapshotterService,
                                                                int epoch,
                                                                Configuration config,
                                                                String taskId) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
                changeEventSourceMetricsFactory, eventDispatcher, schema, signalProcessor, notificationService,
                snapshotterService);
        this.epoch = epoch;
        this.config = config;
        this.taskId = taskId;
        this.snapshotInfoWaitTimeoutMs = connectorConfig.getSmartSnapshotTaskSnapshotInfoWaitTimeoutMs();
        this.snapshotInfoPollIntervalMs = connectorConfig.getSmartSnapshotTaskSnapshotInfoPollIntervalMs();
        // The coupling between this timeout and the leader join-wait timeout is validated at config time in
        // RelationalBaseSourceConnector#validateSmartSnapshotConfig.
    }

    // Visible for testing: shorten the snapshot-info poll interval so tests do not sleep the default.
    protected void setSnapshotInfoPollIntervalMs(long snapshotInfoPollIntervalMs) {
        this.snapshotInfoPollIntervalMs = snapshotInfoPollIntervalMs;
    }

    /**
     * Read the epoch stamped on a previously saved offset, or {@code null} if none is present. Used to detect
     * a stale offset left over from an earlier round so it can be discarded before this round's snapshot.
     */
    protected abstract Integer epochOf(O offset);

    /**
     * Hand the leader's published snapshot info to the connector's smart snapshot source. The connector decodes
     * the consistent position (Postgres LSN, MySQL binlog file/pos/gtids) and parses {@code assignmentForTask}
     * (the raw per-task table slice) using its own identifier interpretation, then stores everything on the
     * source so the subsequent {@link #doSnapshot} reads at the shared point.
     *
     * @param snapshotName       the leader's snapshot name (Postgres exported snapshot); {@code null} for MySQL
     * @param consistentPoint    the shared consistent position, connector-encoded
     * @param snapshotTxId       the transaction id at the consistent point; {@code null} for connectors that
     *                           don't carry one (e.g. MySQL)
     * @param assignmentForTask  this task's raw table slice from the published assignments
     */
    protected abstract void configureSmartSource(SnapshotChangeEventSource<P, O> snapshotSource,
                                                 int epoch,
                                                 String snapshotName,
                                                 String consistentPoint,
                                                 Long snapshotTxId,
                                                 Object assignmentForTask,
                                                 SnapshotCoordinationFacade snapshotCoordination);

    @Override
    protected void executeChangeEventSources(
                                             CdcSourceTaskContext taskContext,
                                             SnapshotChangeEventSource<P, O> snapshotSource,
                                             Offsets<P, O> previousOffsets,
                                             AtomicReference<LoggingContext.PreviousContext> previousLogContext,
                                             ChangeEventSourceContext context)
            throws InterruptedException {

        // This coordinator owns the task-side facade's lifecycle: it is created, started, used (directly and,
        // through configureSmartSource, by the smart snapshot source during doSnapshot) and stopped here, all on
        // this executor thread. Nothing uses it after this method returns, so closing it here also frees the Kafka
        // clients as soon as this task's slice is done instead of holding them while the task idles until the
        // downscale. Creation and start are inside the try, so a facade that fails half-way through either is
        // still closed; a failure propagates to the base coordinator, which fails the task.
        try {
            snapshotCoordination = createCoordination();
            snapshotCoordination.start(SnapshotCoordination.MissingTopicPolicy.FAIL);
            executeSmartSnapshot(taskContext, snapshotSource, previousOffsets, previousLogContext, context);
        }
        finally {
            stopCoordination();
        }
    }

    /**
     * Creates the task-side coordination facade. Called once, on the executor thread, at the start of
     * {@link #executeChangeEventSources}. Tasks never create the coordination topic (they start with
     * {@code FAIL}); the connector provisions it. Visible for testing, so a test can hand in a mock instead of a
     * Kafka-backed facade.
     */
    protected SnapshotCoordinationFacade createCoordination() {
        return new SnapshotCoordinationFacade(config, connectorConfig);
    }

    private void stopCoordination() {
        // null only if creating the facade itself failed; then there is nothing to close
        if (snapshotCoordination == null) {
            return;
        }
        // Same reason as the leader's cleanup: a task stop can interrupt this thread (executor.shutdownNow()), and
        // KafkaBasedLog.stop() join()s its work thread, which throws at once on an interrupted thread and skips
        // closing the producer and consumer. Clear the flag for the close and restore it afterwards.
        boolean wasInterrupted = Thread.interrupted();
        try {
            snapshotCoordination.stop();
        }
        catch (Exception e) {
            LOGGER.warn("{} Failed to cleanly close the coordination facade. error={}",
                    SmartSnapshotLogging.task(taskId, epoch), e.getMessage());
        }
        finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void executeSmartSnapshot(CdcSourceTaskContext taskContext,
                                      SnapshotChangeEventSource<P, O> snapshotSource,
                                      Offsets<P, O> previousOffsets,
                                      AtomicReference<LoggingContext.PreviousContext> previousLogContext,
                                      ChangeEventSourceContext context)
            throws InterruptedException {
        P partition = previousOffsets.getTheOnlyPartition();
        previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));

        O previousOffset = previousOffsets.getTheOnlyOffset();

        // Epoch mismatch with previous offset → stale from previous round, clear it
        if (previousOffset != null) {
            Integer offsetEpoch = epochOf(previousOffset);
            if (offsetEpoch != null && !offsetEpoch.equals(epoch)) {
                LOGGER.info("{} Epoch mismatch, clearing offset. offsetEpoch={} configEpoch={}",
                        SmartSnapshotLogging.task(taskId, epoch), offsetEpoch, epoch);
                previousOffsets.resetOffset(partition);
                previousOffset = null;
            }
        }

        // previousOffset here is non-null only if its epoch == this epoch (the epoch-mismatch block above
        // reset it otherwise). If it shows the snapshot already completed, this restart is just the task
        // being bounced AFTER finishing (a reconfiguration or the managed runtime stopping a "done" task)
        // NOT a crash. Do not treat it as a rejoin; return and let the connector downscale.
        boolean done = snapshotCoordination.isTaskDone(taskId, epoch);
        if (done) {
            LOGGER.info("{} Already completed, waiting for downscale", SmartSnapshotLogging.task(taskId, epoch));
            return;
        }

        // Rejoin detection. Only a task that already STARTED ITS TRANSACTION cannot be resumed after a restart:
        // it attached to the leader's exported snapshot (which may already be released) and, since
        // task_started_transaction is written after the schema read but before any data rows are emitted, it may
        // be mid-slice. That needs a clean round, so signal a full restart.
        //
        // A task that only wrote its join marker but never started its transaction did NOT attach and emitted no
        // data. It can safely re-run at the SAME epoch, so we must NOT force a restart for it. Keying this on the
        // join marker (as before) wrongly bumped the epoch when a task simply died while waiting for the snapshot
        // to be prepared.
        if (snapshotCoordination.hasTaskStartedTransaction(taskId, epoch)) {
            LOGGER.warn("{} Rejoin after transaction start detected, signaling `restart_needed`", SmartSnapshotLogging.task(taskId, epoch));
            writeRestartNeeded();
            return;
        }

        // Stale-epoch check: this task's config epoch is already behind the epoch the connector has persisted.
        // That means the config is stale — a leftover task from an old round, or one assigned during a rebalance
        // before it was reconfigured. Return and wait for the reconfiguration to hand it the current epoch.
        //
        // This must run BEFORE reading the snapshot info. snapshot_info is keyed by server, not by epoch, so a
        // stale task could otherwise find a snapshot whose epoch matches its own stale epoch, attach to it (that
        // snapshot has already been released) and fail. The in-loop epoch check below does NOT cover this: it runs
        // after the snapshot-info match, so a first-iteration match would attach before it ever fires.
        Integer readEpoch = snapshotCoordination.readEpoch();
        if (readEpoch != null && readEpoch > epoch) {
            LOGGER.warn("{} Saved epoch is greater than current epoch, waiting for restart. savedEpoch={} currentEpoch={}",
                    SmartSnapshotLogging.task(taskId, epoch), readEpoch, epoch);
            return;
        }

        // Write the join marker. It tells the leader this task is up so it can wait for all tasks to join before
        // taking locks. It does NOT by itself trigger a restart on a later bounce — that is keyed on
        // task_started_transaction above — so a task that dies here (joined but not yet attached) re-runs cleanly
        // at the same epoch. If this write fails we just fail the task; the task has done nothing to clean up.
        snapshotCoordination.writeTaskJoin(taskId, epoch);

        // Read snapshot info from the coordination topic. Wait up to snapshotInfoWaitTimeoutMs, which must be
        // larger than the leader's join-wait + prepare time so we do not give up before the snapshot is published.
        // The shared poll loop handles the transient-read retry, the throttled "still waiting" logging and the
        // interrupt-aware park (it throws InterruptedException, propagated by this method, when the task is stopped).
        AtomicReference<Map<String, Object>> published = new AtomicReference<>();
        SmartSnapshotPolling.Outcome outcome = SmartSnapshotPolling.pollUntil(
                SmartSnapshotLogging.task(taskId, epoch), "snapshot preparation",
                Duration.ofMillis(snapshotInfoWaitTimeoutMs), Duration.ofMillis(snapshotInfoPollIntervalMs),
                () -> {
                    // null until the leader has published the snapshot info for THIS epoch (a record from another
                    // round, or one without a consistent point yet, also reads as null)
                    Map<String, Object> snapshotInfo = snapshotCoordination.readSnapshotInfo(epoch);
                    if (snapshotInfo != null) {
                        published.set(snapshotInfo);
                        return SmartSnapshotPolling.PollResult.READY;
                    }

                    // The connector may have moved on to a newer epoch (the leader gave up and restarted the round).
                    // If so, there is no point waiting out the full timeout for a snapshot at this epoch — stop
                    // waiting and let this task be reconfigured for the new epoch.
                    Integer newerEpoch = snapshotCoordination.readEpoch();
                    if (newerEpoch != null && newerEpoch > epoch) {
                        LOGGER.warn("{} Connector advanced to a newer epoch while waiting for snapshot info, "
                                + "waiting for restart. savedEpoch={}", SmartSnapshotLogging.task(taskId, epoch), newerEpoch);
                        return SmartSnapshotPolling.PollResult.ABORT;
                    }
                    return SmartSnapshotPolling.PollResult.CONTINUE;
                },
                null);

        if (outcome == SmartSnapshotPolling.Outcome.ABORTED) {
            // The connector is on a newer epoch; this task waits to be reconfigured.
            return;
        }
        if (outcome == SmartSnapshotPolling.Outcome.TIMED_OUT) {
            throw new DebeziumException(SmartSnapshotLogging.task(taskId, epoch) + " Timed out waiting for snapshot preparation");
        }

        Map<String, Object> snapshotInfo = published.get();
        String snapshotName = (String) snapshotInfo.get(SnapshotCoordinationFacade.SNAPSHOT_NAME);
        String consistentPoint = String.valueOf(snapshotInfo.get(SnapshotCoordinationFacade.CONSISTENT_POINT));
        Long snapshotTxId = SnapshotCoordinationFacade.txIdOf(snapshotInfo);
        Object assignmentForTask = SmartSnapshotTableAssignments.assignmentForTask(
                snapshotInfo.get(SnapshotCoordinationFacade.ASSIGNMENTS), Integer.parseInt(taskId));

        LOGGER.info("{} Read snapshot info, executing snapshot-only. snapshot={}, position={}, txId={}",
                SmartSnapshotLogging.task(taskId, epoch), snapshotName, consistentPoint, snapshotTxId);

        // Hand the published info to the connector's smart snapshot source (decode position + parse assignment).
        configureSmartSource(snapshotSource, epoch, snapshotName, consistentPoint, snapshotTxId, assignmentForTask, snapshotCoordination);

        try {
            SnapshotResult<O> snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);
            LOGGER.info("{} Snapshot completed. status={}", SmartSnapshotLogging.task(taskId, epoch), snapshotResult.getStatus());
        }
        catch (InterruptedException e) {
            // Interrupt means the task is being stopped/restarted; the snapshot did NOT complete.
            // Do NOT fall through to writeCompleted() — marking an unfinished subset "done" would let the
            // monitor downscale it and cause isTaskDone() to skip the snapshot on the next restart.
            LOGGER.warn("{} Interrupted during snapshot, exiting gracefully", SmartSnapshotLogging.task(taskId, epoch), e);
            Thread.currentThread().interrupt();
            return;
        }
        catch (Exception e) {
            // An interrupt does not always surface as InterruptedException: a JDBC/socket read or a Kafka
            // producer call can throw a wrapped exception after the interrupt flag was set. Those land here,
            // not in the block above. Treat them like the interrupt path: the task is stopping. On the next
            // start the rejoin path handles cleanup (if it had started its transaction it signals restart;
            // otherwise it re-runs cleanly), and writeRestartNeeded() is a blocking Kafka write that would
            // likely fail under interrupt anyway.
            if (Thread.currentThread().isInterrupted()) {
                LOGGER.warn("{} Interrupted during snapshot (surfaced as {}), exiting gracefully",
                        SmartSnapshotLogging.task(taskId, epoch), e.getClass().getSimpleName(), e);
                return;
            }

            // A real snapshot failure. Here we DO write restart_needed: the task already attached and may have
            // emitted partial data, so the epoch must bump to throw that work away. The topic is likely still up
            // (the failure was in the snapshot, not the write), so the signal should go through and the monitor
            // acts on its next poll. If the write also fails, writeRestartNeeded throws and the marker handles it
            // on restart.
            LOGGER.warn("{} Snapshot failed, signaling `restart_needed`", SmartSnapshotLogging.task(taskId, epoch), e);
            writeRestartNeeded();
            throw new DebeziumException(SmartSnapshotLogging.task(taskId, epoch) + " Snapshot failed, signaling restart_needed", e);
        }

        declareTaskDone();

        // Snapshot-only task: nothing more to do here. Return and let the connector monitor detect all tasks done
        // and downscale/reconfigure this task. The task stays alive (RUNNING, empty polls) until then.
        LOGGER.info("{} Slice complete, waiting for downscale", SmartSnapshotLogging.task(taskId, epoch));
    }

    private void writeRestartNeeded() {
        try {
            snapshotCoordination.writeRestartNeeded(taskId, epoch);
        }
        catch (Exception e) {
            // The topic write failed, so we cannot signal a restart. Just fail the task.
            // On restart the marker is still there, so it tries to signal again. If the topic is still
            // down it keeps failing and restarting until the topic is back, then the signal goes through.
            // Nothing is committed in the meantime, so this is safe.
            throw new DebeziumException(
                    SmartSnapshotLogging.task(taskId, epoch) + " Failed to write restart_needed", e);
        }
    }

    private void declareTaskDone() {
        try {
            snapshotCoordination.writeTaskDone(taskId, epoch);
        }
        catch (Exception e) {
            // can't record completion, the monitor would never downscale; fail so the task retries
            throw new DebeziumException(
                    SmartSnapshotLogging.task(taskId, epoch) + " Failed to write completion", e);
        }
    }
}
