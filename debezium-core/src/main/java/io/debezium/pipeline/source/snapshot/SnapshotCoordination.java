/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.Map;

/**
 * Abstraction for cross-task coordination during multi-task snapshot.
 * The leader writes coordination data (LSN, snapshot name, epoch) to a shared key,
 * and followers poll it to join the leader's snapshot.
 *
 * First implementation uses the Connect offset topic (heartbeat SourceRecords for writes,
 * offsetStorageReader for reads). Can be swapped for direct Kafka topic access or other
 * mechanisms by implementing this interface.
 *
 * Used by tasks only — the Connector's monitor thread reads offsets directly
 * via context().offsetStorageReader().
 */
public interface SnapshotCoordination {
    /**
     * Write coordination data to the shared key {"server":"<prefix>"}.
     * Overwrites the entire value (Connect offsets are full map replacements).
     */
    void writeSharedData(Map<String, Object> data) throws Exception;

    /**
     * Read coordination data from the shared key {"server":"<prefix>"}.
     * Returns null if no data exists yet (leader hasn't written).
     * Subject to flush delay in the offset topic implementation.
     */
    Map<String, Object> readSharedData();
}
