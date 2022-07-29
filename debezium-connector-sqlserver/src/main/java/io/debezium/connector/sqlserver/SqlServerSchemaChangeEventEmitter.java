/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.AbstractMap.SimpleEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Table;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;

/**
 * {@link SchemaChangeEventEmitter} implementation based on SQL Server.
 *
 * @author Jiri Pechanec
 */
public class SqlServerSchemaChangeEventEmitter implements SchemaChangeEventEmitter {

    private final SqlServerPartition partition;
    private final SqlServerOffsetContext offsetContext;
    private final SqlServerChangeTable changeTable;
    private final Table tableSchema;
    private final SchemaChangeEventType eventType;
    private final SimpleEntry<String, String> changeTableSyncInfoPair;

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSchemaChangeEventEmitter.class);

    public SqlServerSchemaChangeEventEmitter(SqlServerPartition partition, SqlServerOffsetContext offsetContext, SqlServerChangeTable changeTable, Table tableSchema,
                                             SchemaChangeEventType eventType, SimpleEntry<String, String> changeTableSyncInfoPair) {
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.changeTable = changeTable;
        this.tableSchema = tableSchema;
        this.eventType = eventType;
        this.changeTableSyncInfoPair = changeTableSyncInfoPair;
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
        final SchemaChangeEvent event = new SchemaChangeEvent(
                partition.getSourcePartition(),
                offsetContext.getOffset(),
                offsetContext.getSourceInfo(),
                changeTable.getSourceTableId().catalog(),
                changeTable.getSourceTableId().schema(),
                "N/A",
                tableSchema,
                eventType,
                false);
        LOGGER.info("event is: {}", event);
        receiver.schemaChangeEvent(event, changeTableSyncInfoPair);
    }
}
