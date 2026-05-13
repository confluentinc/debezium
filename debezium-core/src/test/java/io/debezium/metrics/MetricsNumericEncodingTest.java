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
import java.util.Set;
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

/**
 * Registers a metrics MBean on the platform MBean server with each setting of the
 * {@code metrics.numeric.encoding.enable} config and asserts the JMX attribute shape
 * matches the selected interface.
 */
public class MetricsNumericEncodingTest {

    private SchemaHistoryMetrics metrics;

    @After
    public void cleanup() {
        if (metrics != null) {
            metrics.stopped();
        }
    }

    @Test
    public void schemaHistoryExposesStringStatusWhenFlagIsOff() throws Exception {
        metrics = new SchemaHistoryMetrics(testConfig(false), false);
        metrics.started();

        MBeanInfo info = mbeanInfo();
        assertThat(attributeType(info, "Status")).isEqualTo("java.lang.String");
        assertThat(attributeNames(info)).doesNotContain("StatusCode");
    }

    @Test
    public void schemaHistoryExposesLongStatusCodeWhenFlagIsOn() throws Exception {
        metrics = new SchemaHistoryMetrics(testConfig(true), false);
        metrics.started();

        MBeanInfo info = mbeanInfo();
        assertThat(attributeType(info, "StatusCode")).isEqualTo("long");
        assertThat(attributeNames(info)).doesNotContain("Status");
    }

    @Test
    public void commonAttributesAreExposedInBothEncodings() throws Exception {
        metrics = new SchemaHistoryMetrics(testConfig(true), false);
        metrics.started();

        Set<String> names = attributeNames(mbeanInfo());
        assertThat(names).contains("RecoveryStartTime", "ChangesRecovered", "ChangesApplied",
                "MilliSecondsSinceLastAppliedChange", "MilliSecondsSinceLastRecoveredChange",
                "LastAppliedChange", "LastRecoveredChange");
    }

    private static MBeanInfo mbeanInfo() throws Exception {
        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName(
                "debezium.test:type=connector-metrics,context=schema-history,server=test-server");
        return mbeanServer.getMBeanInfo(name);
    }

    private static String attributeType(MBeanInfo info, String name) {
        for (MBeanAttributeInfo a : info.getAttributes()) {
            if (a.getName().equals(name)) {
                return a.getType();
            }
        }
        throw new AssertionError("attribute " + name + " not exposed; have: " + attributeNames(info));
    }

    private static Set<String> attributeNames(MBeanInfo info) {
        return Arrays.stream(info.getAttributes())
                .map(MBeanAttributeInfo::getName)
                .collect(Collectors.toSet());
    }

    private static CommonConnectorConfig testConfig(boolean numericEncoding) {
        Configuration cfg = Configuration.create()
                .with(CommonConnectorConfig.METRICS_NUMERIC_ENCODING_ENABLE, numericEncoding)
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
