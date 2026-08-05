/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.List;

import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;

public interface SmartSnapshotLifecycleManager {

    ChangeEventSource.ChangeEventSourceContext RUNNING_CONTEXT = new ChangeEventSource.ChangeEventSourceContext() {
        @Override
        public boolean isPaused() {
            return false;
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public void resumeStreaming() {
        }

        @Override
        public void waitSnapshotCompletion() {
        }

        @Override
        public void streamingPaused() {
        }

        @Override
        public void waitStreamingPaused() {
        }
    };

    class SnapshotSetup {
        private final String snapshotName;
        private final String consistentPosition;
        private final List<TableId> tables;

        public SnapshotSetup(String snapshotName, String consistentPosition, List<TableId> tables) {
            this.snapshotName = snapshotName;
            this.consistentPosition = consistentPosition;
            this.tables = tables;
        }

        public String snapshotName() {
            return snapshotName;
        }

        public String consistentPosition() {
            return consistentPosition;
        }

        public List<TableId> tables() {
            return tables;
        }
    }

    /**
     * Create slot/snapshot + lock tables. Holds connections open until releaseSnapshot().
     */
    SnapshotSetup prepareSnapshot(boolean shouldStream);

    /**
     * Called when all tasks have read schema. Postgres: no-op. MySQL: UNLOCK TABLES.
     */
    void onAllTasksStartedTransaction();

    /**
     * Close all held connections and release locks. Idempotent and a no-op when nothing is held: it is called
     * from the leader thread's finally block and from the task-stop thread, possibly both.
     */
    void releaseSnapshot();

    /**
     * Ping held snapshot/lock connections to keep them alive during a long snapshot.
     * Throws if a held connection is dead (the exported snapshot/slot/lock is gone) so the
     * leader task fails fast. No-op if nothing is held.
     */
    void keepAlive();
}
