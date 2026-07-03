/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.source.snapshot.SmartSnapshotConnectorCoordinator;

public class SqlServerOffsetContextTest {

    private SqlServerConnectorConfig configWithEpoch(String epoch) {
        Configuration.Builder builder = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX");
        if (epoch != null) {
            builder = builder.with(SmartSnapshotConnectorCoordinator.EPOCH_KEY, epoch);
        }
        return new SqlServerConnectorConfig(builder.build());
    }

    @Test
    public void offsetOmitsEpochWhenSmartSnapshotDisabled() {
        SqlServerConnectorConfig connectorConfig = configWithEpoch(null);
        SqlServerOffsetContext offset = new SqlServerOffsetContext(
                connectorConfig, TxLogPosition.valueOf(Lsn.valueOf(new byte[]{ 0x01 })), null, false);

        assertThat(offset.getEpoch()).isNull();
        assertThat(offset.getOffset()).doesNotContainKey(SmartSnapshotConnectorCoordinator.EPOCH_KEY);
    }

    @Test
    public void offsetCarriesEpochForShardedSmartSnapshotTask() {
        SqlServerConnectorConfig connectorConfig = configWithEpoch("2");
        SqlServerOffsetContext offset = new SqlServerOffsetContext(
                connectorConfig, TxLogPosition.valueOf(Lsn.valueOf(new byte[]{ 0x01 })), null, false);

        assertThat(offset.getEpoch()).isEqualTo(2);
        assertThat(offset.getOffset().get(SmartSnapshotConnectorCoordinator.EPOCH_KEY)).isEqualTo(2);
    }

    @Test
    public void loaderRoundTripsEpochFromPersistedOffset() {
        SqlServerConnectorConfig connectorConfig = configWithEpoch(null);
        SqlServerOffsetContext original = new SqlServerOffsetContext(
                configWithEpoch("3"), TxLogPosition.valueOf(Lsn.valueOf(new byte[]{ 0x01 })), null, false);

        @SuppressWarnings("unchecked")
        Map<String, Object> persisted = (Map<String, Object>) original.getOffset();

        SqlServerOffsetContext reloaded = new SqlServerOffsetContext.Loader(connectorConfig).load(persisted);

        // the loader trusts the persisted offset's own epoch, not the (possibly stale/bumped) current config
        assertThat(reloaded.getEpoch()).isEqualTo(3);
    }
}
