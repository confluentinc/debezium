/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

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
 */
final class SqlServerUncapturedSchemaCoordination {

    private static final String TYPE = "type";
    private static final String TYPE_UNCAPTURED_SCHEMA = "sqlserver_uncaptured_schema";
    static final String TABLES = "tables";

    private SqlServerUncapturedSchemaCoordination() {
    }

    static Map<String, String> key(String serverName) {
        return Collect.hashMapOf("server", serverName, TYPE, TYPE_UNCAPTURED_SCHEMA);
    }

    static Map<String, Object> value(List<TableId> tables) {
        return Map.of(TABLES, SnapshotCoordinationFacade.joinTableIds(tables));
    }
}
