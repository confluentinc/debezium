/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.history.SchemaHistoryMetrics;

public class NumericMetricsRegistrationTest {

    private SchemaHistoryMetrics metrics;

    @After
    public void cleanup() {
        if (metrics != null) {
            metrics.stopped();
        }
    }

    @Test
    public void schemaHistoryExposesBothStatusAndStatusCode() throws Exception {
        metrics = new SchemaHistoryMetrics(testConfig(), false);
        metrics.started();

        MBeanInfo info = mbeanInfo();
        assertThat(attributeType(info, "Status")).isEqualTo("java.lang.String");
        assertThat(attributeType(info, "StatusCode")).isEqualTo("long");
    }

    @Test
    public void statusCodeReflectsLifecycleTransitions() throws Exception {
        metrics = new SchemaHistoryMetrics(testConfig(), false);
        metrics.started();

        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = objectName();

        assertThat((Long) mbeanServer.getAttribute(name, "StatusCode")).isEqualTo(2L);
        assertThat((String) mbeanServer.getAttribute(name, "Status")).isEqualTo("RUNNING");

        metrics.recoveryStarted();
        assertThat((Long) mbeanServer.getAttribute(name, "StatusCode")).isEqualTo(1L);
        assertThat((String) mbeanServer.getAttribute(name, "Status")).isEqualTo("RECOVERING");

        metrics.recoveryStopped();
        assertThat((Long) mbeanServer.getAttribute(name, "StatusCode")).isEqualTo(2L);
        assertThat((String) mbeanServer.getAttribute(name, "Status")).isEqualTo("RUNNING");
    }

    private static MBeanInfo mbeanInfo() throws Exception {
        return ManagementFactory.getPlatformMBeanServer().getMBeanInfo(objectName());
    }

    private static ObjectName objectName() throws Exception {
        return new ObjectName(
                "debezium.test:type=connector-metrics,context=schema-history,server=test-server");
    }

    private static String attributeType(MBeanInfo info, String name) {
        for (MBeanAttributeInfo a : info.getAttributes()) {
            if (a.getName().equals(name)) {
                return a.getType();
            }
        }
        throw new AssertionError("attribute " + name + " not exposed; have: "
                + Arrays.stream(info.getAttributes()).map(MBeanAttributeInfo::getName).collect(Collectors.toSet()));
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
