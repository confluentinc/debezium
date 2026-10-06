/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.connector.common.OffsetUtils.longOffsetValue;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.SnapshotType;
import io.debezium.connector.binlog.BinlogOffsetContext;
import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;

public class MySqlOffsetContext extends BinlogOffsetContext<SourceInfo> {

    // Smart snapshot round this offset belongs to; null when the feature is off. Round-tripped through the offset
    // map so a restart can detect a stale offset from an earlier round and re-snapshot.
    private final Integer epoch;

    public MySqlOffsetContext(SnapshotType snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                              IncrementalSnapshotContext<TableId> incrementalSnapshotContext, SourceInfo sourceInfo) {
        this(snapshot, snapshotCompleted, transactionContext, incrementalSnapshotContext, sourceInfo, null);
    }

    public MySqlOffsetContext(SnapshotType snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                              IncrementalSnapshotContext<TableId> incrementalSnapshotContext, SourceInfo sourceInfo, Integer epoch) {
        super(snapshot, snapshotCompleted, transactionContext, incrementalSnapshotContext, sourceInfo);
        this.epoch = epoch;
    }

    public static MySqlOffsetContext initial(MySqlConnectorConfig config) {
        return initial(config, null);
    }

    public static MySqlOffsetContext initial(MySqlConnectorConfig config, Integer epoch) {
        final MySqlOffsetContext offset = new MySqlOffsetContext(
                null,
                false,
                new TransactionContext(),
                config.isReadOnlyConnection()
                        ? new MySqlReadOnlyIncrementalSnapshotContext<>()
                        : new SignalBasedIncrementalSnapshotContext<>(),
                new SourceInfo(config),
                epoch);
        offset.setBinlogStartPoint("", 0L); // start from the beginning of the binlog
        return offset;
    }

    public Integer getEpoch() {
        return epoch;
    }

    @Override
    public Map<String, ?> getOffset() {
        if (epoch == null) {
            return super.getOffset();
        }
        // Carry the smart-snapshot epoch alongside the binlog position so it round-trips through connect-offsets.
        final Map<String, Object> offset = new HashMap<>(super.getOffset());
        offset.put(SnapshotCoordinationFacade.EPOCH, epoch);
        return offset;
    }

    public static class Loader extends BinlogOffsetContext.Loader<MySqlOffsetContext> {

        private final MySqlConnectorConfig connectorConfig;

        public Loader(MySqlConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public MySqlOffsetContext load(Map<String, ?> offset) {
            final String binlogFilename = (String) offset.get(SourceInfo.BINLOG_FILENAME_OFFSET_KEY);
            if (binlogFilename == null) {
                throw new ConnectException("Source offset '" + SourceInfo.BINLOG_FILENAME_OFFSET_KEY + "' parameter is missing");
            }
            long binlogPosition = longOffsetValue(offset, SourceInfo.BINLOG_POSITION_OFFSET_KEY);
            // Read the smart-snapshot epoch back so the epoch-mismatch reset can force a re-snapshot on a bump.
            final Object epochVal = offset.get(SnapshotCoordinationFacade.EPOCH);
            final Integer epoch = epochVal != null ? ((Number) epochVal).intValue() : null;
            final MySqlOffsetContext offsetContext = new MySqlOffsetContext(
                    loadSnapshot(offset).orElse(null),
                    loadSnapshotCompleted(offset),
                    TransactionContext.load(offset),
                    connectorConfig.isReadOnlyConnection()
                            ? MySqlReadOnlyIncrementalSnapshotContext.load(offset)
                            : SignalBasedIncrementalSnapshotContext.load(offset),
                    new SourceInfo(connectorConfig),
                    epoch);
            offsetContext.setBinlogStartPoint(binlogFilename, binlogPosition);
            offsetContext.setInitialSkips(longOffsetValue(offset, EVENTS_TO_SKIP_OFFSET_KEY),
                    (int) longOffsetValue(offset, SourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY));
            offsetContext.setCompletedGtidSet((String) offset.get(GTID_SET_KEY)); // may be null
            return offsetContext;
        }
    }
}
