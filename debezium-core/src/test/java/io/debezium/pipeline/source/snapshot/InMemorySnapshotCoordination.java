/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link SnapshotCoordination} test double, reusable by any connector's smart-snapshot tests.
 * <p>
 * Mimics the last-write-wins-per-key semantics of the compacted coordination topic backing
 * {@code KafkaLogSnapshotCoordination}, without requiring a Kafka broker. Reads/writes are immediate.
 * Stored keys and values are defensively copied so callers cannot mutate coordination state by reference.
 */
public class InMemorySnapshotCoordination implements SnapshotCoordination {

    private final Map<Map<String, String>, Map<String, Object>> store = new ConcurrentHashMap<>();
    private volatile boolean started;

    @Override
    public void write(Map<String, String> key, Map<String, Object> data) {
        store.put(new HashMap<>(key), new HashMap<>(data));
    }

    @Override
    public Map<String, Object> read(Map<String, String> key) {
        Map<String, Object> value = store.get(key);
        return value == null ? null : new HashMap<>(value);
    }

    @Override
    public Map<String, Object> readSync(Map<String, String> key) {
        return read(key);
    }

    @Override
    public void start() {
        started = true;
    }

    @Override
    public void stop() {
        started = false;
    }

    public boolean isStarted() {
        return started;
    }

    /** Test helper: number of distinct keys written. */
    public int size() {
        return store.size();
    }
}
