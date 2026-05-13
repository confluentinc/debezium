/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;

/**
 * Exercises the {@code Status} / {@code StatusCode} pair across schema-history lifecycle
 * transitions.
 */
public class SchemaHistoryMetricsTest {

    @Test
    public void initialStatusIsStopped() {
        SchemaHistoryMetrics metrics = new SchemaHistoryMetrics(testConfig(), false);

        assertThat(metrics.getStatus()).isEqualTo("STOPPED");
        assertThat(metrics.getStatusCode()).isEqualTo(0L);
    }

    @Test
    public void recoveryStartedReportsRecovering() {
        SchemaHistoryMetrics metrics = new SchemaHistoryMetrics(testConfig(), false);
        metrics.recoveryStarted();

        assertThat(metrics.getStatus()).isEqualTo("RECOVERING");
        assertThat(metrics.getStatusCode()).isEqualTo(1L);
    }

    @Test
    public void recoveryStoppedTransitionsToRunning() {
        SchemaHistoryMetrics metrics = new SchemaHistoryMetrics(testConfig(), false);
        metrics.recoveryStarted();
        metrics.recoveryStopped();

        assertThat(metrics.getStatus()).isEqualTo("RUNNING");
        assertThat(metrics.getStatusCode()).isEqualTo(2L);
    }

    @Test
    public void stoppedAfterStartedReturnsToStopped() {
        SchemaHistoryMetrics metrics = new SchemaHistoryMetrics(testConfig(), false);
        metrics.stopped();

        assertThat(metrics.getStatus()).isEqualTo("STOPPED");
        assertThat(metrics.getStatusCode()).isEqualTo(0L);
    }

    private static CommonConnectorConfig testConfig() {
        Configuration cfg = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "test-server")
                .build();
        return new TestConnectorConfig(cfg);
    }

    private static final class TestConnectorConfig extends CommonConnectorConfig {

        private TestConnectorConfig(Configuration config) {
            super(config, 0);
        }

        @Override
        public String getContextName() {
            return "test";
        }

        @Override
        public String getConnectorName() {
            return "test";
        }

        @Override
        public EnumeratedValue getSnapshotMode() {
            return null;
        }

        @Override
        public Optional<EnumeratedValue> getSnapshotLockingMode() {
            return Optional.empty();
        }

        @Override
        protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
            return null;
        }
    }
}
