/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.junit.SkipTestRule;
import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * Tests for {@link KafkaLogSnapshotCoordination} against an embedded Kafka broker (same style as
 * {@code KafkaClusterTest}). Covers the compacted read/write round trip, latest-value-per-key, and the
 * idempotent topic creation / start that the multi-task snapshot relies on.
 */
public class KafkaLogSnapshotCoordinationTest {

    @Rule
    public TestRule skipTestRule = new SkipTestRule();

    private static final String SERVER = "srv";

    private KafkaCluster cluster;
    private File dataDir;
    private CommonConnectorConfig connectorConfig;
    private Configuration config;
    private final List<KafkaLogSnapshotCoordination> started = new ArrayList<>();

    @Before
    public void beforeEach() throws Exception {
        dataDir = Testing.Files.createTestingDirectory("coordination-cluster");
        cluster = new KafkaCluster().usingDirectory(dataDir)
                .deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true)
                .addBrokers(1)
                .startup();

        connectorConfig = mock(CommonConnectorConfig.class);
        when(connectorConfig.getLogicalName()).thenReturn(SERVER);
        when(connectorConfig.getSmartSnapshotCoordinationBootstrapServers()).thenReturn(cluster.brokerList());
        config = Configuration.create().build();
    }

    @After
    public void afterEach() {
        for (KafkaLogSnapshotCoordination coordination : started) {
            try {
                coordination.stop();
            }
            catch (Exception ignored) {
                // best effort cleanup
            }
        }
        cluster.shutdown();
        Testing.Files.delete(dataDir);
    }

    private KafkaLogSnapshotCoordination newCoordination() {
        KafkaLogSnapshotCoordination coordination = new KafkaLogSnapshotCoordination(config, connectorConfig);
        started.add(coordination);
        coordination.start();
        return coordination;
    }

    @Test
    public void aFreshReaderSeesAPreviouslyWrittenRecord() throws Exception {
        Map<String, String> key = Collect.hashMapOf("server", SERVER, "type", "epoch");
        KafkaLogSnapshotCoordination writer = newCoordination();
        writer.write(key, Collect.hashMapOf("epoch", 7));

        // a second client tails the log from the beginning; read() forces a readToEnd, so it reflects the value
        KafkaLogSnapshotCoordination reader = newCoordination();
        assertThat(reader.read(key)).isNotNull();
        assertThat(reader.read(key)).containsEntry("epoch", 7);
    }

    @Test
    public void readCatchesUpToWritesMadeAfterTheReaderStarted() throws Exception {
        Map<String, String> key = Collect.hashMapOf("server", SERVER, "type", "epoch");
        // the reader starts first and catches up to an (empty) topic
        KafkaLogSnapshotCoordination reader = newCoordination();

        // a separate client writes AFTER the reader has already started tailing
        KafkaLogSnapshotCoordination writer = newCoordination();
        writer.write(key, Collect.hashMapOf("epoch", 9));

        // read() forces a readToEnd, so it reflects the just-written value immediately -- no polling/await needed.
        // The old eventually-consistent read could return null here until the background consumer caught up.
        assertThat(reader.read(key)).containsEntry("epoch", 9);
    }

    @Test
    public void latestValuePerKeyWins() throws Exception {
        Map<String, String> key = Collect.hashMapOf("server", SERVER, "type", "epoch");
        KafkaLogSnapshotCoordination writer = newCoordination();
        writer.write(key, Collect.hashMapOf("epoch", 1));
        writer.write(key, Collect.hashMapOf("epoch", 2));

        // read() forces a readToEnd, so it reflects the compacted latest-per-key
        KafkaLogSnapshotCoordination reader = newCoordination();
        assertThat(reader.read(key)).isNotNull();
        assertThat(reader.read(key).get("epoch")).isEqualTo(Integer.valueOf(2));
        assertThat(reader.read(key)).containsEntry("epoch", 2);
    }

    @Test
    public void readReturnsNullForAnUnknownKey() {
        KafkaLogSnapshotCoordination coordination = newCoordination();
        assertThat(coordination.read(Collect.hashMapOf("server", SERVER, "type", "missing"))).isNull();
    }

    @Test
    public void topicCreationIsIdempotent() {
        newCoordination(); // creates the topic
        // a second instance must reuse the existing topic, not fail on TopicExistsException
        assertThatCode(this::newCoordination).doesNotThrowAnyException();
    }

    @Test
    public void startIsIdempotent() throws Exception {
        Map<String, String> key = Collect.hashMapOf("server", SERVER, "type", "epoch");
        KafkaLogSnapshotCoordination coordination = newCoordination();
        coordination.start(); // second start must be a no-op, not re-initialise or throw

        coordination.write(key, Collect.hashMapOf("epoch", 5));
        assertThat(coordination.read(key)).containsEntry("epoch", 5);
    }
}
