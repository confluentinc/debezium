/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.List;

import io.debezium.relational.TableId;

public interface SnapshotLifecycleManager extends AutoCloseable {

    class SnapshotSetup {
        private final String snapshotName;
        private final String consistentPosition;

        public SnapshotSetup(String snapshotName, String consistentPosition) {
            this.snapshotName = snapshotName;
            this.consistentPosition = consistentPosition;
        }

        public String snapshotName() {
            return snapshotName;
        }

        public String consistentPosition() {
            return consistentPosition;
        }
    }

    /**
     * Create slot/snapshot + lock tables. Holds connections open until releaseSnapshot().
     */
    SnapshotSetup prepareSnapshot(List<TableId> tables, boolean shouldStream);

    /**
     * Called when all tasks have read schema. Postgres: no-op. MySQL: UNLOCK TABLES.
     */
    void onAllTasksJoined();

    /**
     * Close all held connections and release locks.
     */
    void releaseSnapshot();

    /**
     * Current snapshot name, or null if not prepared.
     */
    String snapshotName();

    /**
     * Slot LSN (Postgres) or binlog position (MySQL), or null if not prepared.
     */
    String consistentPosition();

    /**
     * Ping held snapshot/lock connections to keep them alive during a long snapshot.
     * Throws if a held connection is dead (the exported snapshot/slot/lock is gone) so the
     * leader task fails fast. No-op if nothing is held.
     */
    void keepAlive();

    @Override
    default void close() {
        releaseSnapshot();
    }
}
