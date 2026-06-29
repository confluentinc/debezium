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

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

public class SmartSnapshotConnectorCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmartSnapshotConnectorCoordinator.class);
    private static final long MONITOR_POLL_INTERVAL_MS = 30_000;

    public static final String EPOCH_KEY = "epoch";
    public static final String SNAPSHOT_NAME_KEY = "snapshot_name";
    public static final String SLOT_LSN_KEY = "slot_lsn";
    public static final String ALL_TABLES_KEY = "smart.snapshot.all.tables";
    public static final String RESTART_NEEDED_KEY = "restart_needed";

    // A record on the coordination topic can carry a "type" so two records for the same server
    // don't overwrite each other. The epoch record and the per-task join marker use it.
    private static final String TYPE = "type";
    private static final String TYPE_EPOCH = "epoch";
    private static final String TYPE_JOIN = "join";
    public static final String COMPLETED_KEY = "completed";
    private static final String TYPE_DONE = "done";

    private final SnapshotCoordination snapshotCoordination;
    private final ConnectorContext connectorContext;
    private final String serverName;

    /*
     * Performs 2 monitoring operations:
     * 1. check if any task has signalled for a restart, if yes set snapshot state to RESTART and invoke
     * connectorContext#requestTaskReconfiguration this will cause the snapshot to be restarted with
     * a higher epoch
     * 2. check if all task have completed their snapshot, if yes set snapshotState to COMPLETE
     * and invoke connectorContext#requestTaskReconfiguration this will cause task downscale
     * and streaming to begin
     *
     * check startMonitorThread()
     */
    private Thread monitorThread;

    /*
     * current coordination round
     * for the first time it is set in the start() method by the information read from the coordination topic
     * it is then propagated to the task as in task configs
     * it is updated in taskConfigs() when it detected the snapshotState to be RESTART
     */
    private final AtomicInteger currentEpoch = new AtomicInteger(0);

    /*
     * the number of tasks
     * taskConfigs() sets lastNumTasks to numTasks
     * after that, the monitor knows how many per-task keys to
     * check for completion.
     */
    private volatile int lastNumTasks;

    /*
     * Last epoch which caused a restart
     * Multiple task can signal restart concurrently
     * However, reconfiguration should be triggered only once per epoch
     */
    private volatile int lastHandledRestartEpoch = -1;
    private volatile List<TableId> snapshotTables;

    enum SmartSnapshotState {
        // smart snapshot is ongoing
        ACTIVE,
        // smart snapshot is completed
        COMPLETE,
        // smart snapshot needs to be restarted from scratch
        // (could be re-started due to task failure or issue in snapshot holder connection)
        RESTART
    }

    // current snapshot state
    private volatile SmartSnapshotState smartSnapshotState = SmartSnapshotState.ACTIVE;

    public SmartSnapshotConnectorCoordinator(SnapshotCoordination snapshotCoordination,
                                             ConnectorContext connectorContext,
                                             String serverName) {
        this.snapshotCoordination = snapshotCoordination;
        this.connectorContext = connectorContext;
        this.serverName = serverName;
    }

    public void start(List<TableId> tables) {
        SourceConnectorContext srcContext = (SourceConnectorContext) connectorContext;
        Map<String, Object> existingOffset = srcContext.offsetStorageReader().offset(snapshotInfoKey());
        boolean offsetExists = (existingOffset != null);
        boolean snapshotInProgress = offsetExists && isSnapshotInProgress(existingOffset);

        if (offsetExists && !snapshotInProgress) {
            LOGGER.info("Smart snapshot: existing streaming offset present, skipping smart snapshot");
            this.smartSnapshotState = SmartSnapshotState.COMPLETE;
            return;
        }

        snapshotCoordination.start();

        Map<String, Object> snapshotInfo = snapshotCoordination.read(snapshotInfoKey());
        if (snapshotInfo != null
                && Boolean.TRUE.equals(snapshotInfo.get(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY))) {
            LOGGER.info("Smart snapshot: coordination topic shows snapshot completed, skipping");
            this.smartSnapshotState = SmartSnapshotState.COMPLETE;
            return;
        }

        // Read the epoch the Connector saved earlier. Do not change it on a plain restart.
        // (The snapshot prep runs on task-0 after taskConfigs, so the snapshot record may not exist
        // yet at this point — that's why the Connector keeps the epoch in its own record.)
        this.currentEpoch.set(determineEpoch(snapshotCoordination.read(epochKey())));
        persistEpoch(currentEpoch.get());

        this.snapshotTables = new ArrayList<>(tables);
        startMonitorThread();
    }

    /**
     * Builds the task list. The same inputs (topic + offsets + maxTasks) always give the same output.
     */
    public List<Map<String, String>> taskConfigs(int maxTasks, Map<String, String> baseProps) {
        List<TableId> tables = snapshotTables;
        if (tables == null || tables.isEmpty()) {
            return null;
        }

        switch (smartSnapshotState) {
            case COMPLETE:
                LOGGER.info("Smart snapshot: complete, writing completion + downscaling");
                writeCompletion();
                return null;
            case RESTART:
                int past = currentEpoch.get();
                int next = currentEpoch.incrementAndGet();
                persistEpoch(next); // save the new epoch before handing out configs
                LOGGER.info("Smart snapshot: epoch restart {} -> {}", past, next);
                this.smartSnapshotState = SmartSnapshotState.ACTIVE;
                break;
            case ACTIVE:
                break;
        }

        List<TableId> sorted = new ArrayList<>(tables);
        sorted.sort(Comparator.comparing(TableId::toString));
        int numTasks = Math.min(maxTasks, sorted.size());
        this.lastNumTasks = numTasks;

        List<List<TableId>> tablesByTask = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            tablesByTask.add(new ArrayList<>());
        }
        for (int i = 0; i < sorted.size(); i++) {
            tablesByTask.get(i % numTasks).add(sorted.get(i));
        }

        String allTables = sorted.stream().map(TableId::toString).collect(Collectors.joining(","));

        List<Map<String, String>> out = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            String subset = tablesByTask.get(i).stream().map(TableId::toString).collect(Collectors.joining(","));
            Map<String, String> taskProps = new HashMap<>(baseProps);
            taskProps.put(CommonConnectorConfig.SNAPSHOT_MODE_TABLES.name(), subset);
            taskProps.put(ConfigurationNames.TASK_ID_PROPERTY_NAME, String.valueOf(i));
            taskProps.put(EPOCH_KEY, String.valueOf(currentEpoch.get()));
            if (i == 0) {
                // task-0 needs the full table list to lock every table before the snapshot
                taskProps.put(ALL_TABLES_KEY, allTables);
            }
            LOGGER.info("Smart snapshot: task {} subset=[{}] epoch={}{}",
                    i, subset, currentEpoch.get(), i == 0 ? " (leader, full table list attached)" : "");
            out.add(taskProps);
        }
        return out;
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
                if (lastNumTasks <= 0) {
                    continue;
                }

                int epoch = currentEpoch.get();

                // 1. restart_needed — always checked, before the completion check; act on each epoch only once
                boolean restart = false;
                for (int i = 0; i < lastNumTasks; i++) {
                    Map<String, Object> sig = snapshotCoordination.read(
                            Collect.hashMapOf("server", serverName, "task", String.valueOf(i)));
                    if (sig != null && Boolean.TRUE.equals(sig.get(RESTART_NEEDED_KEY))) {
                        Integer e = readEpoch(sig);
                        if (e != null && e == epoch && epoch > lastHandledRestartEpoch) {
                            LOGGER.info("Smart snapshot: task {} restart_needed for the epoch {}", i, epoch);
                            restart = true;
                            break;
                        }
                    }
                }
                if (restart) {
                    lastHandledRestartEpoch = epoch;
                    smartSnapshotState = SmartSnapshotState.RESTART;
                    connectorContext.requestTaskReconfiguration();
                    continue;
                }

                // 2. all complete for the epoch → downscale
                SourceConnectorContext src = (SourceConnectorContext) connectorContext;
                boolean allComplete = true;
                for (int i = 0; i < lastNumTasks; i++) {
                    Map<String, Object> done = snapshotCoordination.read(completedKey(serverName, String.valueOf(i)));
                    Integer doneEpoch = readEpoch(done);
                    if (done == null || !Boolean.TRUE.equals(done.get(COMPLETED_KEY)) || doneEpoch == null || doneEpoch != epoch) {
                        allComplete = false;
                        break;
                    }
                }
                if (allComplete) {
                    LOGGER.info("Smart snapshot: all {} tasks complete for the epoch {}, downscaling", lastNumTasks, epoch);
                    smartSnapshotState = SmartSnapshotState.COMPLETE;
                    connectorContext.requestTaskReconfiguration();
                    return;
                }
            }
        }, "smart-snapshot-monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    public boolean isComplete() {
        return smartSnapshotState == SmartSnapshotState.COMPLETE;
    }

    public void stop() {
        stopMonitorThread();
        snapshotCoordination.stop();
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

    private void writeCompletion() {
        try {
            Map<String, Object> snapshotInfo = snapshotCoordination.read(snapshotInfoKey());
            Map<String, Object> data = new HashMap<>();
            data.put(SLOT_LSN_KEY, snapshotInfo != null ? snapshotInfo.get(SLOT_LSN_KEY) : null);
            data.put(CommonOffsetContext.SNAPSHOT_COMPLETED_KEY, true);
            data.put(EPOCH_KEY, currentEpoch.get());
            snapshotCoordination.write(snapshotInfoKey(), data);
        }
        catch (Exception e) {
            LOGGER.error("Smart snapshot: failed to write completion", e);
        }
    }

    private void persistEpoch(int epoch) {
        try {
            Map<String, Object> record = new HashMap<>();
            record.put(EPOCH_KEY, epoch);
            snapshotCoordination.write(epochKey(), record);
        }
        catch (Exception e) {
            LOGGER.error("Smart snapshot: failed to save epoch {}", epoch, e);
        }
    }

    public int currentEpoch() {
        return currentEpoch.get();
    }

    /**
     * Read the saved epoch; default to 1 on the first ever start. Never changes it here.
     */
    private int determineEpoch(Map<String, Object> epochRecord) {
        Integer existing = readEpoch(epochRecord);
        return existing != null ? existing : 1;
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

    // where the snapshot name + LSN are stored (written by task-0 once the snapshot is ready)
    private Map<String, String> snapshotInfoKey() {
        return Collect.hashMapOf("server", serverName);
    }

    // where the Connector saves the epoch number
    private Map<String, String> epochKey() {
        return epochKey(serverName);
    }

    public static Map<String, String> epochKey(String serverName) {
        return Collect.hashMapOf("server", serverName, TYPE, TYPE_EPOCH);
    }

    // where a task writes transaction_started / restart_needed
    public static Map<String, String> taskSignalKey(String serverName, String taskId) {
        return Collect.hashMapOf("server", serverName, "task", taskId);
    }

    // where a task writes its join marker (proof it started this epoch)
    public static Map<String, String> joinMarkerKey(String serverName, String taskId) {
        return Collect.hashMapOf("server", serverName, "task", taskId, TYPE, TYPE_JOIN);
    }

    public static Map<String, String> completedKey(String serverName, String taskId) {
        return Collect.hashMapOf("server", serverName, "task", taskId, TYPE, TYPE_DONE);
    }
}
