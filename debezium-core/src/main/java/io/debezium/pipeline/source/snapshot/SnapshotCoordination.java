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

    /**
     * Start reaching the coordination topic. {@code policy} decides what happens when the topic is missing:
     * assume it exists, skip quietly, or fail fast (see {@link MissingTopicPolicy}).
     *
     * @return true if started; false only when the topic is missing and the policy is {@link MissingTopicPolicy#SKIP}
     */
    boolean start(SnapshotCoordination.MissingTopicPolicy policy);

    void stop();

    /**
     * What {start(MissingTopicPolicy)} should do when the coordination topic is missing.
     */
    enum MissingTopicPolicy {

        /**
         * Assume the topic exists and just start. Used by the connector, which creates the topic itself.
         */
        ASSUME_EXISTS,

        /**
         * Return false without starting. Used by read-only lookups that skip quietly when there is nothing to read.
         */
        SKIP,

        /**
         * Throw. Used by tasks, which never create the topic and must fail fast if the connector has not provisioned it.
         */
        FAIL
    }
}