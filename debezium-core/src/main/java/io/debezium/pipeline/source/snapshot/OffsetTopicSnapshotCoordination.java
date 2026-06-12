/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.heartbeat.HeartbeatImpl;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.util.Collect;

/**
 * SnapshotCoordination implementation that uses the Connect offset topic.
 * Writes via heartbeat SourceRecords, reads via offsetStorageReader().
 * Subject to Connect's flush delay (offset.flush.interval.ms, default 60s).
 */
public class OffsetTopicSnapshotCoordination implements SnapshotCoordination {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetTopicSnapshotCoordination.class);

    private final ChangeEventQueue<DataChangeEvent> queue;
    private final OffsetStorageReader offsetStorageReader;
    private final Map<String, String> sharedPartition;
    private final String heartbeatTopicName;
    private final String serverName;

    public OffsetTopicSnapshotCoordination(
                                           ChangeEventQueue<DataChangeEvent> queue,
                                           OffsetStorageReader offsetStorageReader,
                                           String serverName,
                                           String heartbeatTopicName) {
        this.queue = queue;
        this.offsetStorageReader = offsetStorageReader;
        this.sharedPartition = Collect.hashMapOf("server", serverName);
        this.serverName = serverName;
        this.heartbeatTopicName = heartbeatTopicName;
    }

    @Override
    public void write(Map<String, String> key, Map<String, Object> data) throws Exception {
        SchemaNameAdjuster adjuster = SchemaNameAdjuster.NO_OP;
        Schema keySchema = SchemaFactory.get().heartbeatKeySchema(adjuster);
        Schema valueSchema = SchemaFactory.get().heartbeatValueSchema(adjuster);

        Struct structKey = new Struct(keySchema);
        structKey.put(HeartbeatImpl.SERVER_NAME_KEY, serverName);

        Struct value = new Struct(valueSchema);
        value.put(AbstractSourceInfo.TIMESTAMP_KEY, System.currentTimeMillis());

        SourceRecord record = new SourceRecord(
                sharedPartition,
                data,
                heartbeatTopicName,
                null,
                keySchema,
                key,
                valueSchema,
                value,
                null);

        queue.enqueue(new DataChangeEvent(record));
        LOGGER.info("Smart snapshot: Coordination data written to shared key {}, data={}",
                sharedPartition, data);
    }

    @Override
    public Map<String, Object> read(Map<String, String> key) {
        return offsetStorageReader.offset(sharedPartition);
    }

    @Override
    public void start() {
        // no-op
    }

    @Override
    public void stop() {
        // no-op
    }
}
