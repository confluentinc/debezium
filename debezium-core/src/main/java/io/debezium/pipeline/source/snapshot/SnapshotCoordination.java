/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.Map;

/**
 * Abstraction for cross-process coordination during multi-task snapshot.
 */
public interface SnapshotCoordination {

    void write(Map<String, String> key, Map<String, Object> data) throws Exception;

    Map<String, Object> read(Map<String, String> key);

    void start();

    void stop();
}