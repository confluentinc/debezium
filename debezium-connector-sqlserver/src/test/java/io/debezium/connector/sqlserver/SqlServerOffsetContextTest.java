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
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * Unit tests for the smart-snapshot epoch stamped on a sharded task's offset: it must survive the
 * {@code getOffset()} -> {@code Loader.load()} round-trip (so a restarted shard can detect a stale-epoch offset)
 * and must be absent entirely when smart snapshot is not engaged (backward compatibility).
 */
public class SqlServerOffsetContextTest {

    private static final Lsn LSN = Lsn.valueOf("0000002b:00000cc0:001a");

    private SqlServerConnectorConfig config() {
        return new SqlServerConnectorConfig(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                .with(SqlServerConnectorConfig.DATABASE_NAMES, "db1")
                .build());
    }

    private SqlServerOffsetContext offsetWithEpoch(Integer epoch) {
        return new SqlServerOffsetContext(config(), TxLogPosition.valueOf(LSN), null, false, 1,
                new io.debezium.pipeline.txmetadata.TransactionContext(),
                new SqlServerIncrementalSnapshotContext<>(), epoch);
    }

    @Test
    public void getOffsetOmitsEpochKeyWhenNull() {
        Map<String, ?> offset = offsetWithEpoch(null).getOffset();
        assertThat(offset).doesNotContainKey(SnapshotCoordinationFacade.EPOCH);
    }

    @Test
    public void getOffsetIncludesEpochWhenSet() {
        Map<String, ?> offset = offsetWithEpoch(7).getOffset();
        assertThat(offset).containsKey(SnapshotCoordinationFacade.EPOCH);
        assertThat(offset.get(SnapshotCoordinationFacade.EPOCH)).isEqualTo(7);
    }

    @Test
    public void epochRoundTripsThroughLoader() {
        Map<String, ?> stored = offsetWithEpoch(3).getOffset();
        OffsetContext.Loader<SqlServerOffsetContext> loader = new SqlServerOffsetContext.Loader(config());
        assertThat(loader.load(stored).getEpoch()).isEqualTo(3);
    }

    @Test
    public void loaderYieldsNullEpochWhenAbsent() {
        Map<String, ?> stored = offsetWithEpoch(null).getOffset();
        OffsetContext.Loader<SqlServerOffsetContext> loader = new SqlServerOffsetContext.Loader(config());
        assertThat(loader.load(stored).getEpoch()).isNull();
    }
}
