/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.pipeline.source.snapshot.SnapshotCoordinationFacade;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

/**
 * A SQL-Server-only coordination record, written alongside (but outside of) the shared
 * {@code SnapshotCoordinationFacade}'s typed {@code snapshot_info} record: the "eligible for schema-history
 * but not in the data-capture set" leftover table list (design §6.2), only non-empty under
 * {@code store.only.captured.tables.ddl=false} (the default). The shared facade has no extension point for
 * connector-specific fields, so this piggybacks a second key onto the same connector-wide coordination topic
 * rather than forking the framework class.
 *
 * <p>Carries an {@code EPOCH} tag (gap fix, mirrors {@code snapshot_info}'s own EPOCH field) so a reader can
 * tell a value published for its own round apart from one left over from a since-superseded epoch: the
 * Connector republishes this record on every restart that finds an incomplete round (same trigger as the
 * epoch bump in {@code SqlServerConnector#bumpEpochIfIncompleteRoundExists}), so an old task-0 instance still
 * winding down under a stale epoch, or a new task-0 racing ahead of Kafka replication lag before the fresh
 * publish lands, must not silently use a value meant for a different epoch.
 */
final class SqlServerUncapturedSchemaCoordination {

    private static final String TYPE = "type";
    private static final String TYPE_UNCAPTURED_SCHEMA = "sqlserver_uncaptured_schema";
    static final String TABLES = "tables";
    static final String EPOCH = "epoch";

    private SqlServerUncapturedSchemaCoordination() {
    }

    static Map<String, String> key(String serverName) {
        return Collect.hashMapOf("server", serverName, TYPE, TYPE_UNCAPTURED_SCHEMA);
    }

    static Map<String, Object> value(List<TableId> tables, int epoch) {
        Map<String, Object> value = new HashMap<>();
        value.put(TABLES, SnapshotCoordinationFacade.joinTableIds(tables));
        value.put(EPOCH, epoch);
        return value;
    }

    /**
     * {@code null} when the record is missing, or was never tagged with an epoch (defensive; every writer
     * post-gap-fix always tags one) -- both treated as "not usable" by the reader.
     */
    static Integer epochOf(Map<String, Object> value) {
        Object epoch = value.get(EPOCH);
        return epoch instanceof Number ? ((Number) epoch).intValue() : null;
    }
}
