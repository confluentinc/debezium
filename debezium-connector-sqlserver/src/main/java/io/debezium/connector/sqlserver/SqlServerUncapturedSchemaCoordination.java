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
 * A SQL-Server-only coordination record piggybacked onto the connector-wide coordination topic (the shared
 * {@code SnapshotCoordinationFacade} has no extension point for connector-specific fields): the "eligible for
 * schema-history but not in the data-capture set" leftover table list, only non-empty under
 * {@code store.only.captured.tables.ddl=false}. Carries an {@code EPOCH} tag so a reader can reject a value
 * left over from a since-superseded round (a connector restart bumps the epoch and republishes).
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

    /** {@code null} when the value carries no epoch tag, treated by the reader as "not usable". */
    static Integer epochOf(Map<String, Object> value) {
        Object epoch = value.get(EPOCH);
        return epoch instanceof Number ? ((Number) epoch).intValue() : null;
    }
}
