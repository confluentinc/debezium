/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.util.Collect;

/**
 * SnapshotCoordination implementation that uses the Connect offset topic.
 * Writes via heartbeat SourceRecords, reads via offsetStorageReader().
 * Subject to Connect's flush delay (offset.flush.interval.ms, default 60s).
 */
public class OffsetTopicSnapshotCoordination implements SnapshotCoordination {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(OffsetTopicSnapshotCoordination.class);

    private final ChangeEventQueue<DataChangeEvent> queue;
    private final OffsetStorageReader offsetStorageReader;
    private final Map<String, String> sharedPartition;
    private final String heartbeatTopicName;

    public OffsetTopicSnapshotCoordination(
            ChangeEventQueue<DataChangeEvent> queue,
            OffsetStorageReader offsetStorageReader,
            String serverName,
            String heartbeatTopicName) {
        this.queue = queue;
        this.offsetStorageReader = offsetStorageReader;
        this.sharedPartition = Collect.hashMapOf("server", serverName);
        this.heartbeatTopicName = heartbeatTopicName;
    }

    @Override
    public void writeSharedData(Map<String, Object> data) throws Exception {
        SourceRecord record = new SourceRecord(
                sharedPartition,
                data,
                heartbeatTopicName,
                null,
                Schema.OPTIONAL_STRING_SCHEMA,
                "smart-snapshot-coordination",
                Schema.OPTIONAL_STRING_SCHEMA,
                "coordination",
                null);

        queue.enqueue(new DataChangeEvent(record));
        LOGGER.info("Smart snapshot: wrote coordination data to shared key {}, data={}",
                sharedPartition, data);
    }

    @Override
    public Map<String, Object> readSharedData() throws Exception {
        return offsetStorageReader.offset(sharedPartition);
    }
}
