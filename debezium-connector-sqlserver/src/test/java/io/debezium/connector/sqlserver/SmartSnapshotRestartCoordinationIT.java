/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.awaitility.Awaitility;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;

/**
 * Deterministic coordination-level test of the "one task failure -> full restart" contract, without the embedded
 * engine or a SQL Server snapshot round (both of which make the end-to-end restart path racy): a shard writes
 * {@code restart_needed} and the connector's monitor bumps the epoch, handing out the new epoch on the next
 * {@code taskConfigs}. Uses the real SQL Server {@link SnapshotCoordinationFacade} against a real Kafka broker
 * plus a minimal {@link SourceConnectorContext} that fulfils reconfiguration by re-running {@code taskConfigs},
 * exactly as the Connect runtime does. (The monitor's internal state machine is covered more exhaustively by
 * {@code SmartSnapshotConnectorCoordinatorTest} in debezium-core.)
 *
 * <p>Requires a real Kafka broker (default {@code 127.0.0.1:9092}, override via
 * {@code test.smart.snapshot.coordination.bootstrap.servers}).
 */
public class SmartSnapshotRestartCoordinationIT {

    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getProperty(
            "test.smart.snapshot.coordination.bootstrap.servers", "127.0.0.1:9092");

    @Test
    public void restartNeededBumpsEpochOnNextTaskConfigs() {
        String serverName = "server" + UUID.randomUUID().toString().replace("-", "");
        Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, serverName)
                .with(SqlServerConnectorConfig.DATABASE_NAMES, "db1")
                .with("producer.override.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .with("admin.override.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .build();
        SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        SnapshotCoordinationFacade facade = new SnapshotCoordinationFacade(config, connectorConfig);

        Map<String, String> baseProps = new HashMap<>();
        baseProps.put("connector.class", "x");
        AtomicReference<SmartSnapshotConnectorCoordinator> ref = new AtomicReference<>();
        AtomicReference<Throwable> raised = new AtomicReference<>();

        SourceConnectorContext context = new SourceConnectorContext() {
            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        return null; // fresh: no existing streaming offset
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                        return Collections.emptyMap();
                    }
                };
            }

            @Override
            public void requestTaskReconfiguration() {
                // fulfil the reconfiguration the way the Connect runtime does -> this is where the epoch bumps
                ref.get().taskConfigs(2, baseProps);
            }

            @Override
            public void raiseError(Exception e) {
                raised.set(e);
            }
        };

        SmartSnapshotConnectorCoordinator coordinator = new SmartSnapshotConnectorCoordinator(
                facade, context, serverName, 200L, 60_000L, "test");
        ref.set(coordinator);
        try {
            coordinator.start();
            assertThat(coordinator.taskConfigs(2, baseProps).get(0))
                    .containsEntry(SnapshotCoordinationFacade.EPOCH, "1")
                    .containsEntry(SnapshotCoordinationFacade.NUM_TASKS, "2");

            facade.writeRestartNeeded("0", 1); // a shard signals it needs a full restart

            Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(
                    () -> assertThat(coordinator.taskConfigs(2, baseProps).get(0))
                            .containsEntry(SnapshotCoordinationFacade.EPOCH, "2"));
            assertThat(raised.get()).isNull();
        }
        finally {
            coordinator.stop();
            facade.stop();
        }
    }
}
