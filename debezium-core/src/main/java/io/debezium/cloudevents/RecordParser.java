/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.cloudevents;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * An abstract parser of change records. Callers {@link #create(Schema, Object) create} a concrete parser for a record
 * and the parser's type is chosen by the connector type (e.g. mysql, postgresql, etc.) defined in change records.
 * Fields and metadata of change records can be provided by RecordParser.
 */
public abstract class RecordParser {

    private Object before;
    private Object after;
    private Struct source;
    private String op;
    private String ts_ms;
    private Schema beforeSchema;
    private Schema afterSchema;
    private String connectorType;

    static final Set<String> SOURCE_FIELDS = Collect.unmodifiableSet(
            AbstractSourceInfo.DEBEZIUM_VERSION_KEY,
            AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY,
            AbstractSourceInfo.SERVER_NAME_KEY,
            AbstractSourceInfo.TIMESTAMP_KEY,
            AbstractSourceInfo.SNAPSHOT_KEY,
            AbstractSourceInfo.DATABASE_NAME_KEY);

    /**
     * Create a concrete parser of a change record for a specific connector type.
     *
     * @param schema the schema of the record
     * @param value the value of the record
     * @return a concrete parser
     */
    public static RecordParser create(Schema schema, Object value) {
        Struct record = requireStruct(value, "CloudEvents converter");
        String connectorType = record.getStruct(Envelope.FieldName.SOURCE).getString(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY);

        switch (connectorType) {
            case "mysql":
                return new MysqlRecordParser(schema, record);
            case "postgresql":
                return new PostgresRecordParser(schema, record);
            case "mongodb":
                return new MongodbRecordParser(schema, record);
            case "sqlserver":
                return new SqlserverRecordParser(schema, record);
            default:
                throw new DataException("No usable CloudEvents converters for connector type \"" + connectorType + "\"");
        }
    }

    RecordParser(Schema schema, Struct record) {
        parse(schema, record);
    }

    private void parse(Schema schema, Struct record) {
        before = schema.field(Envelope.FieldName.BEFORE) == null ? null : record.get(Envelope.FieldName.BEFORE);
        after = schema.field(Envelope.FieldName.AFTER) == null ? null : record.get(Envelope.FieldName.AFTER);
        source = record.getStruct(Envelope.FieldName.SOURCE);
        op = record.getString(Envelope.FieldName.OPERATION);
        ts_ms = record.getInt64(Envelope.FieldName.TIMESTAMP).toString();
        beforeSchema = before == null ? null : schema.field(Envelope.FieldName.BEFORE).schema();
        afterSchema = after == null ? null : schema.field(Envelope.FieldName.AFTER).schema();
        connectorType = source.getString(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY);
    }

    /**
     * Get the value of the before field in the record; may be null.
     *
     * @return the value of the before field
     */
    public Object before() {
        return before;
    }

    /**
     * Get the value of the after field in the record; may be null.
     *
     * @return the value of the after field
     */
    public Object after() {
        return after;
    }

    /**
     * Get the value of the source field in the record.
     *
     * @return the value of the source field
     */
    public Struct source() {
        return source;
    }

    /**
     * Get the value of the op field in the record.
     *
     * @return the value of the op field
     */
    public String op() {
        return op;
    }

    /**
     * Get the value of the ts_ms field in the record.
     *
     * @return the value of the ts_ms field
     */
    public String ts_ms() {
        return ts_ms;
    }

    /**
     * Get the schema of the before field in the record; may be null.
     *
     * @return the schema of the before field
     */
    public Schema beforeSchema() {
        return beforeSchema;
    }

    /**
     * Get the schema of the after field in the record; may be null.
     *
     * @return the schema of the after field
     */
    public Schema afterSchema() {
        return afterSchema;
    }

    /**
     * Get the type of the connector which produced this record
     *.
     * @return the connector type
     */
    public String connectorType() {
        return connectorType;
    }

    /**
     * Search for metadata of the record by name, which are defined in the source field; throw a DataException if not
     * found.
     *
     * @return metadata of the record
     */
    public abstract Object getMetadata(String name);

    /**
     * Parser for records produced by MySQL connectors.
     */
    public static final class MysqlRecordParser extends RecordParser {
        static final String TABLE_NAME_KEY = "table";
        static final String SERVER_ID_KEY = "server_id";
        static final String GTID_KEY = "gtid";
        static final String BINLOG_FILENAME_OFFSET_KEY = "file";
        static final String BINLOG_POSITION_OFFSET_KEY = "pos";
        static final String BINLOG_ROW_IN_EVENT_OFFSET_KEY = "row";
        static final String THREAD_KEY = "thread";
        static final String QUERY_KEY = "query";

        static final Set<String> MYSQL_SOURCE_FIELDS = Collect.unmodifiableSet(
                TABLE_NAME_KEY,
                SERVER_ID_KEY,
                GTID_KEY,
                BINLOG_FILENAME_OFFSET_KEY,
                BINLOG_POSITION_OFFSET_KEY,
                BINLOG_ROW_IN_EVENT_OFFSET_KEY,
                THREAD_KEY,
                QUERY_KEY);

        MysqlRecordParser(Schema schema, Struct record) {
            super(schema, record);
        }

        @Override
        public Object getMetadata(String name) {
            if (SOURCE_FIELDS.contains(name)) {
                return source().get(name);
            }
            if (MYSQL_SOURCE_FIELDS.contains(name)) {
                return source().get(name);
            }

            throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from MySQL connector");
        }
    }

    /**
     * Parser for records produced by PostgreSQL connectors.
     */
    public static final class PostgresRecordParser extends RecordParser {
        static final String TXID_KEY = "txId";
        static final String XMIN_KEY = "xmin";
        static final String LSN_KEY = "lsn";

        static final Set<String> POSTGRES_SOURCE_FIELD = Collect.unmodifiableSet(
                TXID_KEY,
                XMIN_KEY,
                LSN_KEY);

        PostgresRecordParser(Schema schema, Struct record) {
            super(schema, record);
        }

        @Override
        public Object getMetadata(String name) {
            if (SOURCE_FIELDS.contains(name)) {
                return source().get(name);
            }
            if (POSTGRES_SOURCE_FIELD.contains(name)) {
                return source().get(name);
            }

            throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from PostgreSQL connector");
        }
    }

    /**
     * Parser for records produced by MongoDB connectors.
     */
    public static final class MongodbRecordParser extends RecordParser {
        static final String REPLICA_SET_NAME = "rs";
        static final String ORDER = "ord";
        static final String OPERATION_ID = "h";
        static final String COLLECTION = "collection";

        static final Set<String> MONGODB_SOURCE_FIELD = Collect.unmodifiableSet(
                REPLICA_SET_NAME,
                ORDER,
                OPERATION_ID,
                COLLECTION);

        MongodbRecordParser(Schema schema, Struct record) {
            super(schema, record);
        }

        @Override
        public Object getMetadata(String name) {
            if (SOURCE_FIELDS.contains(name)) {
                return source().get(name);
            }
            if (MONGODB_SOURCE_FIELD.contains(name)) {
                return source().get(name);
            }

            throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from MongoDB connector");
        }
    }

    /**
     * Parser for records produced by Sql Server connectors.
     */
    public static final class SqlserverRecordParser extends RecordParser {
        static final String CHANGE_LSN_KEY = "change_lsn";
        static final String COMMIT_LSN_KEY = "commit_lsn";
        static final String EVENT_SERIAL_NO_KEY = "event_serial_no";

        static final Set<String> SQLSERVER_SOURCE_FIELD = Collect.unmodifiableSet(
                CHANGE_LSN_KEY,
                COMMIT_LSN_KEY,
                EVENT_SERIAL_NO_KEY);

        SqlserverRecordParser(Schema schema, Struct record) {
            super(schema, record);
        }

        @Override
        public Object getMetadata(String name) {
            if (SOURCE_FIELDS.contains(name)) {
                return source().get(name);
            }
            if (SQLSERVER_SOURCE_FIELD.contains(name)) {
                return source().get(name);
            }

            throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from SQLServer connector");
        }
    }
}
