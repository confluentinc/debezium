/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.relational.TableId;

public class SmartSnapshotConnectorCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmartSnapshotConnectorCoordinator.class);
    private static final long MONITOR_POLL_INTERVAL_MS = 30_000;

    public static final String EPOCH_KEY = "epoch";
    public static final String SNAPSHOT_NAME_KEY = "snapshot_name";
    public static final String SLOT_LSN_KEY = "slot_lsn";

    private final SnapshotCoordination snapshotCoordination;
    private final SnapshotLifecycleManager snapshotLifecycleManager;
    private final ConnectorContext connectorContext;
    private final String serverName;
    /*
     * Optional database scope. Single-database connectors (Postgres) leave this null, keeping the
     * coordination/offset keys as {server[,task]}. Multi-database connectors (SQL Server) instantiate one
     * coordinator per database and pass the database here so all keys become {server,database[,task]},
     * matching the per-(server,database,task) offset partitions the tasks actually write.
     */
    private final String database;

    /*
     * Performs 4 monitoring operations:
     * 1. check if the snapshot preparation thread failed, if yes raise error causing the connector to be restarted
     * 2. check the snapshot connection health, in case the connection dies it changes the snapshotState to RESTART
     * and invoke connectorContext#requestTaskReconfiguration this will cause the snapshot to be restarted with
     * a higher epoch
     * 3. check if all the task have signalled transaction_started, if yes invoke snapshotLifecycleManager#onAllTaskJoined
     * 4. check if any task has signalled for a restart, if yes set snapshot state to RESTART
     * and invoke connectorContext#requestTaskReconfiguration this will cause the snapshot to be restarted with
     * a higher epoch
     * 5. check if all task have completed their snapshot, if yes set snapshotState to COMPLETE
     * and invoke connectorContext#requestTaskReconfiguration this will cause task downscale and streaming to begin
     *
     * check startMonitorThread()
     */
    private Thread monitorThread;

    /*
     * This thread manages creation of snapshot and writing the snapshot info to the coordination topic
     * for the tasks to discover the snapshot details and attach to it
     * Connector specific operations are performed via SnapshotLifecycleManager#prepareSnapshot
     * for Postgres this would involve, slot creation or snapshot creation
     * check startSnapshotPreparation()
     * called in start() during the connector startup
     * called in taskConfigs() when the snapshotState is RESTART
     */
    private volatile Thread snapshotPreparationThread;

    /*
     * Stores any error raised in the preparation thread,
     * monitor thread checks this and triggers connector restart
     */
    private volatile Throwable snapshotPreparationError;

    /*
     * the number of tasks lately
     * taskConfigs() sets lastNumTasks it to numTasks
     * after that, the monitor knows how many per-task keys to
     * check for completion/transaction_started.
     */
    private volatile int lastNumTasks;

    /*
     * current coordination round
     * for the first time it is set in the start() method by the information read from the coordination topic
     * it is then propagated to the task as in task configs
     * it is updated in taskConfigs() when it detected the snapshotState to be RESTART
     */
    private final AtomicInteger currentEpoch = new AtomicInteger(0);

    /*
     * set to true by monitor when all task have joined the snapshot by writing
     * transaction_started to the coordination topic
     */
    private volatile boolean allTasksJoined;

    // tables to be snapshot, set during connector start
    private volatile List<TableId> snapshotTables;

    enum SmartSnapshotState {
        // smart snapshot is ongoing
        ACTIVE,
        // smart snapshot is completed
        COMPLETE,
        // smart snapshot needs to be restarted from scratch
        // (could be started due to task failure or issue in snapshot holder connection)
        RESTART
    }

    // current snapshot state
    private volatile SmartSnapshotState smartSnapshotState = SmartSnapshotState.ACTIVE;

    public SmartSnapshotConnectorCoordinator(SnapshotCoordination snapshotCoordination,
                                             SnapshotLifecycleManager snapshotLifecycleManager,
                                             ConnectorContext connectorContext,
                                             String serverName) {
        this(snapshotCoordination, snapshotLifecycleManager, connectorContext, serverName, null);
    }

    public SmartSnapshotConnectorCoordinator(SnapshotCoordination snapshotCoordination,
                                             SnapshotLifecycleManager snapshotLifecycleManager,
                                             ConnectorContext connectorContext,
                                             String serverName,
                                             String database) {
        this.snapshotCoordination = snapshotCoordination;
        this.snapshotLifecycleManager = snapshotLifecycleManager;
        this.connectorContext = connectorContext;
        this.serverName = serverName;
        this.database = database;
    }

    /**
     * Coordination/offset key for the shared (per-database) record: {server[,database]}.
     */
    private Map<String, String> sharedKey() {
        Map<String, String> key = new HashMap<>();
        key.put("server", serverName);
        if (database != null) {
            key.put("database", database);
        }
        return key;
    }

    /**
     * Coordination/offset key for a per-task record: {server[,database],task}.
     */
    private Map<String, String> taskKey(int taskIndex) {
        Map<String, String> key = sharedKey();
        key.put("task", String.valueOf(taskIndex));
        return key;
    }

    /**
     * Called from Connector.start(). Determines initial epoch, kicks off
     * background snapshot preparation, starts monitor thread.
     */
    public void start(List<TableId> tables, boolean shouldStream) {
        // Use same logic as single-task code (InitialSnapshotter.shouldSnapshotData):
        // offsetExists && !snapshotInProgress → don't snapshot (already streaming)
        // This handles streaming offsets where snapshot_completed field is absent
        SourceConnectorContext srcContext = (SourceConnectorContext) connectorContext;
        Map<String, Object> existingOffset = srcContext.offsetStorageReader().offset(
                sharedKey());
        boolean offsetExists = existingOffset != null;
        boolean snapshotInProgress = offsetExists && isSnapshotInProgress(existingOffset);

        if (offsetExists && !snapshotInProgress) {
            LOGGER.info("Smart snapshot: snapshotter says no snapshot needed, skipping");
            this.smartSnapshotState = SmartSnapshotState.COMPLETE;
            return;
        }

        snapshotCoordination.start();

        // Check coordination topic: if previous multi-task snapshot completed
        Map<String, String> sharedPartition = sharedKey();
        Map<String, Object> sharedOffset = snapshotCoordination.read(sharedPartition);

        if (sharedOffset != null
                && Boolean.TRUE.equals(sharedOffset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY))) {
            LOGGER.info("Smart snapshot: coordination topic shows snapshot completed, skipping");
            this.smartSnapshotState = SmartSnapshotState.COMPLETE;
            return;
        }

        this.currentEpoch.set(determineEpoch(sharedOffset));

        // Prepare snapshot in background
        startSnapshotPreparation(tables, shouldStream);

        // Start monitor
        startMonitorThread();
    }

    public boolean isComplete() {
        return smartSnapshotState == SmartSnapshotState.COMPLETE;
    }

    /**
     * Called from Connector.taskConfigs(), returns multi-task configs, or null if snapshot complete/not ready.
     */
    public List<Map<String, String>> taskConfigs(
                                                 int maxTasks,
                                                 Map<String, String> baseProps,
                                                 boolean shouldStream) {
        List<TableId> tables = snapshotTables;
        if (tables == null || tables.isEmpty()) {
            return null;
        }

        switch (smartSnapshotState) {
            case COMPLETE:
                LOGGER.info("Smart snapshot: snapshot complete, releasing");
                String finalPosition = snapshotLifecycleManager.consistentPosition();
                snapshotLifecycleManager.releaseSnapshot();
                // Write completion to coordination topic (streaming task reads LSN from here)
                try {
                    Map<String, String> sp = sharedKey();
                    Map<String, Object> coordData = new HashMap<>();
                    coordData.put(SLOT_LSN_KEY, finalPosition);
                    coordData.put(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY, true);
                    coordData.put(EPOCH_KEY, currentEpoch.get());
                    snapshotCoordination.write(sp, coordData);
                }
                catch (Exception e) {
                    LOGGER.error("Failed to write completion to coordination topic", e);
                }
                // Caller returns single streaming config
                return null;

            case RESTART:
                int pastEpoch = currentEpoch.get();
                this.currentEpoch.incrementAndGet();
                LOGGER.info("Smart snapshot: epoch restart, epoch {} → {}", pastEpoch, currentEpoch.get());
                snapshotLifecycleManager.releaseSnapshot();
                startSnapshotPreparation(tables, shouldStream);
                this.smartSnapshotState = SmartSnapshotState.ACTIVE;
                // Fall through to build multi-task configs
                break;

            case ACTIVE:
                // Build multi-task configs
                break;
        }

        // Distribute tables round-robin
        tables = new ArrayList<>(tables);
        tables.sort(Comparator.comparing(TableId::toString));
        int numTasks = Math.min(maxTasks, tables.size());
        this.lastNumTasks = numTasks;
        this.allTasksJoined = false;

        List<List<TableId>> tablesByTask = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            tablesByTask.add(new ArrayList<>());
        }
        for (int i = 0; i < tables.size(); i++) {
            tablesByTask.get(i % numTasks).add(tables.get(i));
        }

        List<Map<String, String>> taskConfigsList = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            String snapshotTables = tablesByTask.get(i).stream()
                    .map(TableId::toString)
                    .collect(Collectors.joining(","));

            Map<String, String> taskProps = new HashMap<>(baseProps);
            taskProps.put(CommonConnectorConfig.SNAPSHOT_MODE_TABLES.name(), snapshotTables);
            taskProps.put(ConfigurationNames.TASK_ID_PROPERTY_NAME, String.valueOf(i));
            taskProps.put(EPOCH_KEY, String.valueOf(currentEpoch.get()));

            LOGGER.info("Smart snapshot: task {} tables=[{}], epoch={}", i, snapshotTables, currentEpoch);
            taskConfigsList.add(taskProps);
        }

        return taskConfigsList;
    }

    public void stop() {
        if (snapshotPreparationThread != null) {
            snapshotPreparationThread.interrupt();
            try {
                snapshotPreparationThread.join(5000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            snapshotPreparationThread = null;
        }
        stopMonitorThread();
        snapshotLifecycleManager.releaseSnapshot();
        snapshotCoordination.stop();
    }

    private void startSnapshotPreparation(List<TableId> tables, boolean shouldStream) {
        // Cancel previous background thread if still running
        if (snapshotPreparationThread != null) {
            snapshotPreparationThread.interrupt();
            try {
                snapshotPreparationThread.join(5000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        this.snapshotPreparationError = null;
        this.snapshotTables = new ArrayList<>(tables);

        snapshotPreparationThread = new Thread(() -> {
            try {
                SnapshotLifecycleManager.SnapshotSetup setup = snapshotLifecycleManager.prepareSnapshot(tables, shouldStream);

                // Write snapshot info to coordination topic
                Map<String, String> sharedPartition = sharedKey();
                Map<String, Object> coordData = new HashMap<>();
                coordData.put(SLOT_LSN_KEY, setup.consistentPosition());
                coordData.put(SNAPSHOT_NAME_KEY, setup.snapshotName());
                coordData.put(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY, false);
                coordData.put(EPOCH_KEY, currentEpoch.get());
                snapshotCoordination.write(sharedPartition, coordData);

                LOGGER.info("Smart snapshot: preparation complete, snapshot='{}', LSN={}, epoch={}",
                        setup.snapshotName(), setup.consistentPosition(), currentEpoch.get());
            }
            catch (Exception e) {
                LOGGER.error("Smart snapshot: preparation failed", e);
                snapshotLifecycleManager.releaseSnapshot();
                snapshotPreparationError = e;
            }
        }, "smart-snapshot-prepare");
        snapshotPreparationThread.setDaemon(true);
        snapshotPreparationThread.start();
    }

    private void startMonitorThread() {
        monitorThread = new Thread(() -> {
            LOGGER.info("Smart snapshot: monitor thread started");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(MONITOR_POLL_INTERVAL_MS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                // skip until taskConfigs() sets it
                if (lastNumTasks <= 0) {
                    continue;
                }

                // 1. Check if background preparation failed, raise error
                if (snapshotPreparationError != null) {
                    LOGGER.error("Smart snapshot: preparation failed, raising error");

                    /*
                     * if the background preparation thread, since this is a daemon thread
                     * if we throw from here the current thread will silently die
                     * we want to actually want the connector to be restarted in this scenario
                     *
                     * this isn't use anywhere in debezium, reason being that most of the work that is
                     * done within the coordinator thread handles exception by setting the exception on
                     * errorHandler.setProducerThrowable(e), finally in task poll errorHandler checks and throws
                     *
                     * but doing the same is not possible from this thread
                     */
                    connectorContext.raiseError(new DebeziumException(
                            "Smart snapshot: preparation failed", snapshotPreparationError));
                    return;
                }

                // 2. Check held connection health
                if (snapshotLifecycleManager != null && !snapshotLifecycleManager.isValid()) {
                    LOGGER.warn("Smart snapshot: held connection died, setting RESTART state");
                    smartSnapshotState = SmartSnapshotState.RESTART;
                    connectorContext.requestTaskReconfiguration();
                    continue;
                }

                // 3. Check if all tasks joined, call onAllTasksJoined()
                if (!allTasksJoined && snapshotLifecycleManager != null) {
                    boolean allJoined = true;
                    for (int i = 0; i < lastNumTasks; i++) {
                        Map<String, String> taskPartition = taskKey(i);
                        Map<String, Object> taskCoord = snapshotCoordination.read(taskPartition);
                        if (taskCoord == null
                                || !Boolean.TRUE.equals(taskCoord.get("transaction_started"))) {
                            allJoined = false;
                            break;
                        }
                    }
                    if (allJoined) {
                        LOGGER.info("Smart snapshot: all tasks joined");
                        snapshotLifecycleManager.onAllTasksJoined();
                        allTasksJoined = true;
                    }
                }

                // 4. Check per-task restart_needed (MySQL: task can't rejoin snapshot)
                if (snapshotLifecycleManager.requiresFullRestartOnTaskFailure()) {
                    for (int i = 0; i < lastNumTasks; i++) {
                        Map<String, String> taskPartition = taskKey(i);
                        Map<String, Object> taskCoord = snapshotCoordination.read(taskPartition);
                        if (taskCoord != null
                                && Boolean.TRUE.equals(taskCoord.get("restart_needed"))) {
                            LOGGER.info("Smart snapshot: task {} signaled restart_needed", i);
                            smartSnapshotState = SmartSnapshotState.RESTART;
                            connectorContext.requestTaskReconfiguration();
                            break;
                        }
                    }
                    if (smartSnapshotState == SmartSnapshotState.RESTART) {
                        continue;
                    }
                }

                // 5. Check all tasks completed
                SourceConnectorContext srcContext = (SourceConnectorContext) connectorContext;
                boolean allComplete = true;
                Map<String, String> sharedPartition = sharedKey();
                Map<String, Object> sharedOffset = snapshotCoordination.read(sharedPartition);
                Integer expectedEpoch = readEpoch(sharedOffset);

                for (int i = 0; i < lastNumTasks; i++) {
                    Map<String, String> taskPartition = taskKey(i);
                    Map<String, Object> taskOffset = srcContext.offsetStorageReader().offset(taskPartition);
                    if (taskOffset == null) {
                        allComplete = false;
                        break;
                    }
                    boolean done = Boolean.TRUE.equals(
                            taskOffset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY));
                    Integer taskEpoch = readEpoch(taskOffset);
                    if (!done || (expectedEpoch != null && !expectedEpoch.equals(taskEpoch))) {
                        allComplete = false;
                        break;
                    }
                }

                if (allComplete) {
                    LOGGER.info("Smart snapshot: all {} tasks completed, setting COMPLETE state", lastNumTasks);
                    smartSnapshotState = SmartSnapshotState.COMPLETE;
                    connectorContext.requestTaskReconfiguration();
                    return;
                }
            }
        }, "smart-snapshot-monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    private void stopMonitorThread() {
        if (monitorThread != null) {
            monitorThread.interrupt();
            try {
                monitorThread.join(5000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            monitorThread = null;
        }
    }

    private int determineEpoch(Map<String, Object> sharedOffset) {
        if (sharedOffset == null) {
            return 1;
        }
        Integer existingEpoch = readEpoch(sharedOffset);
        boolean completed = Boolean.TRUE.equals(
                sharedOffset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY));
        if (!completed) {
            // Previous round incomplete → new epoch
            return (existingEpoch != null ? existingEpoch : 0) + 1;
        }
        return existingEpoch != null ? existingEpoch : 1;
    }

    private static boolean isSnapshotInProgress(Map<String, Object> offset) {
        Object snapshot = offset.get(AbstractSourceInfo.SNAPSHOT_KEY);
        boolean completed = Boolean.TRUE.equals(offset.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY));
        return snapshot != null && !completed;
    }

    public static Integer readEpoch(Map<String, Object> offset) {
        return (offset != null && offset.get(EPOCH_KEY) != null)
                ? ((Number) offset.get(EPOCH_KEY)).intValue()
                : null;
    }
}
