/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotLockingMode;

/**
 * Unit tests for {@link MySqlConnector#smartSnapshotApplies(Configuration)} — smart snapshot must engage only for
 * the default {@code minimal} locking mode and a parallelizable initial-data snapshot mode; every other
 * combination must fall back to the single-task path.
 */
public class MySqlSmartSnapshotEnablementTest {

    private static Configuration config(SnapshotMode snapshotMode, SnapshotLockingMode lockingMode, boolean smartEnabled) {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "test_server")
                .with(BinlogConnectorConfig.HOSTNAME, "localhost")
                .with(BinlogConnectorConfig.USER, "user")
                .with(BinlogConnectorConfig.PASSWORD, "pass")
                // satisfies the JDBC credentials provider during config construction
                .with("jdbc.creds.provider.user", "user")
                .with("jdbc.creds.provider.password", "pass")
                .with(CommonConnectorConfig.SMART_SNAPSHOT_ENABLED, smartEnabled)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, snapshotMode.getValue())
                .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, lockingMode.getValue())
                .build();
    }

    @Test
    public void engagesOnlyForMinimalLockingAndParallelizableSnapshotModes() {
        // default (minimal) locking + parallelizable snapshot modes -> engages
        assertThat(MySqlConnector.smartSnapshotApplies(config(SnapshotMode.INITIAL, SnapshotLockingMode.MINIMAL, true))).isTrue();
        assertThat(MySqlConnector.smartSnapshotApplies(config(SnapshotMode.INITIAL_ONLY, SnapshotLockingMode.MINIMAL, true))).isTrue();
        assertThat(MySqlConnector.smartSnapshotApplies(config(SnapshotMode.WHEN_NEEDED, SnapshotLockingMode.MINIMAL, true))).isTrue();
    }

    @Test
    public void doesNotEngageForNonMinimalLockingModes() {
        assertThat(MySqlConnector.smartSnapshotApplies(config(SnapshotMode.INITIAL, SnapshotLockingMode.EXTENDED, true))).isFalse();
        assertThat(MySqlConnector.smartSnapshotApplies(config(SnapshotMode.INITIAL, SnapshotLockingMode.NONE, true))).isFalse();
        assertThat(MySqlConnector.smartSnapshotApplies(config(SnapshotMode.INITIAL, SnapshotLockingMode.MINIMAL_PERCONA, true))).isFalse();
        assertThat(MySqlConnector.smartSnapshotApplies(config(SnapshotMode.INITIAL, SnapshotLockingMode.MINIMAL_PERCONA_NO_TABLE_LOCKS, true))).isFalse();
    }

    @Test
    public void doesNotEngageForNonParallelizableSnapshotModes() {
        assertThat(MySqlConnector.smartSnapshotApplies(config(SnapshotMode.ALWAYS, SnapshotLockingMode.MINIMAL, true))).isFalse();
        assertThat(MySqlConnector.smartSnapshotApplies(config(SnapshotMode.NO_DATA, SnapshotLockingMode.MINIMAL, true))).isFalse();
        assertThat(MySqlConnector.smartSnapshotApplies(config(SnapshotMode.NEVER, SnapshotLockingMode.MINIMAL, true))).isFalse();
    }

    @Test
    public void doesNotEngageWhenFeatureDisabled() {
        assertThat(MySqlConnector.smartSnapshotApplies(config(SnapshotMode.INITIAL, SnapshotLockingMode.MINIMAL, false))).isFalse();
    }
}
