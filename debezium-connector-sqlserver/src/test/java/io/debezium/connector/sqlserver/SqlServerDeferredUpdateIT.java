/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.DataQueryMode;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration tests for how the SQL Server connector captures <b>deferred updates</b>.
 * <p>
 * SQL Server's CDC mechanism cannot always represent an {@code UPDATE} as a single before/after
 * pair ({@code __$operation} 3/4). If the statement writes a column that is part of a
 * <b>unique</b> index or constraint, SQL Server executes (and CDC records) a physical
 * <b>delete followed by an insert</b> ({@code __$operation} 1 then 2). This is called a
 * <i>deferred update</i>, and it happens for plain {@code UPDATE} statements and for
 * {@code MERGE} statements alike &mdash; the trigger is which <i>column</i> gets written, not
 * which DML statement is used.
 * <p>
 * This matters operationally: a real customer bulk update that rewrote a unique "business key"
 * column (INC-11206) surfaced as a connector bug in {@code data.query.mode=direct}, because that
 * mode's query ordering can separate a deferred update's delete from its insert and drop the
 * insert. {@code data.query.mode=function} (the connector default, and the only supported mode
 * today) orders strictly by the CDC sequence value, which keeps every deferred update's
 * delete/insert pair together, so no data is lost. These tests pin that guarantee down for
 * {@code function} mode with bulk statements, and exist as a regression net so a future change
 * doesn't reintroduce loss into the supported mode.
 * <p>
 * To make the cause-and-effect obvious, the tests are written as two "minimal diff" pairs. Each
 * pair reuses the exact same table and seed data, and changes only <i>which column</i> the bulk
 * statement writes:
 * <ul>
 * <li>{@link #bulkUpdateOfUniqueIndexedColumn_isCapturedAsDeferredDeleteInsert()} (deferred update
 * <b>seen</b>) vs. {@link #bulkUpdateOfPlainColumn_onSameTable_staysNormalUpdate()} (deferred
 * update <b>not</b> seen) &mdash; same table, plain {@code UPDATE} statement, only the touched
 * column differs.</li>
 * <li>{@link #bulkMergeWritingUniqueKeyColumn_isCapturedAsDeferredDeleteInsert()} (deferred update
 * <b>seen</b>) vs. {@link #bulkMergeNotWritingUniqueKeyColumn_onSameTable_staysNormalUpdate()}
 * (deferred update <b>not</b> seen) &mdash; same table, {@code MERGE} statement, only the touched
 * column differs.</li>
 * </ul>
 */
public class SqlServerDeferredUpdateIT extends AbstractAsyncEngineConnectorTest {

    private static final int ROW_COUNT = 5;

    private static final String ORDERS_DDL = "CREATE TABLE dbo.orders (id INT NOT NULL PRIMARY KEY, order_ref VARCHAR(50) NOT NULL, status VARCHAR(20) NOT NULL);" +
            "CREATE UNIQUE NONCLUSTERED INDEX UX_orders_order_ref ON dbo.orders(order_ref);";
    private static final String ORDERS_SEED = "INSERT INTO dbo.orders VALUES (1,'REF-1','NEW'),(2,'REF-2','NEW'),(3,'REF-3','NEW'),(4,'REF-4','NEW'),(5,'REF-5','NEW');";

    private static final String ACCOUNTS_DDL = "CREATE TABLE dbo.accounts (id INT NOT NULL PRIMARY KEY, account_code INT NOT NULL, balance INT NOT NULL, " +
            "CONSTRAINT UQ_accounts_code UNIQUE(account_code));";
    private static final String ACCOUNTS_SEED = "INSERT INTO dbo.accounts VALUES (1,10,100),(2,20,100),(3,30,100),(4,40,100),(5,50,100);";

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    // ---------------------------------------------------------------------------------------
    // Pair 1: plain UPDATE, writing a uniquely-indexed column vs. a plain column
    // ---------------------------------------------------------------------------------------

    @Test
    public void bulkUpdateOfUniqueIndexedColumn_isCapturedAsDeferredDeleteInsert() throws Exception {
        createAndEnableCdc(ORDERS_DDL, ORDERS_SEED, "orders");

        start(SqlServerConnector.class, ordersConnectorConfig());
        assertConnectorIsRunning();
        consumeRecordsByTopic(ROW_COUNT); // snapshot creates for the 5 seed rows

        // order_ref is covered by a UNIQUE index, so SQL Server cannot rewrite it in place.
        // It performs (and CDC records) a physical delete of the
        // old row plus an insert of the new one -- a deferred update -- instead of the usual
        // before/after update pair.
        connection.execute("UPDATE dbo.orders SET order_ref = order_ref + '_v2' WHERE id BETWEEN 1 AND 5;");

        List<SourceRecord> topicRecords = consumeRecordsByTopic(3 * ROW_COUNT).recordsForTopic(topicName("orders"));
        assertThat(topicRecords).hasSize(3 * ROW_COUNT);

        Map<Integer, Struct> deletesById = valuesByOperation(topicRecords, Envelope.Operation.DELETE);
        Map<Integer, Struct> insertsById = valuesByOperation(topicRecords, Envelope.Operation.CREATE);
        assertThat(deletesById).hasSize(ROW_COUNT);
        assertThat(insertsById).hasSize(ROW_COUNT);
        assertThat(valuesByOperation(topicRecords, Envelope.Operation.UPDATE)).isEmpty();

        for (int id = 1; id <= ROW_COUNT; id++) {
            assertThat(before(deletesById.get(id)).getString("order_ref")).isEqualTo("REF-" + id);
            assertThat(after(insertsById.get(id)).getString("order_ref")).isEqualTo("REF-" + id + "_v2");
        }

        stopConnector();
    }

    @Test
    public void bulkUpdateOfPlainColumn_onSameTable_staysNormalUpdate() throws Exception {
        createAndEnableCdc(ORDERS_DDL, ORDERS_SEED, "orders");

        start(SqlServerConnector.class, ordersConnectorConfig());
        assertConnectorIsRunning();
        consumeRecordsByTopic(ROW_COUNT);

        // Related control for the case above: same table, same seed data, same bulk UPDATE shape --
        // but "status" carries no index, so SQL Server updates the row in place. Whether CDC reports
        // a normal update or a deferred delete+insert is decided purely by which column is written,
        // not by the table, the statement type, or the number of rows touched.
        connection.execute("UPDATE dbo.orders SET status = 'PROCESSED' WHERE id BETWEEN 1 AND 5;");

        List<SourceRecord> topicRecords = consumeRecordsByTopic(ROW_COUNT).recordsForTopic(topicName("orders"));
        assertThat(topicRecords).hasSize(ROW_COUNT);

        Map<Integer, Struct> updatesById = valuesByOperation(topicRecords, Envelope.Operation.UPDATE);
        assertThat(updatesById).hasSize(ROW_COUNT);
        assertThat(valuesByOperation(topicRecords, Envelope.Operation.DELETE)).isEmpty();
        assertThat(valuesByOperation(topicRecords, Envelope.Operation.CREATE)).isEmpty();

        for (int id = 1; id <= ROW_COUNT; id++) {
            Struct value = updatesById.get(id);
            assertThat(before(value).getString("status")).isEqualTo("NEW");
            assertThat(after(value).getString("status")).isEqualTo("PROCESSED");
        }

        stopConnector();
    }

    // ---------------------------------------------------------------------------------------
    // Pair 2: MERGE, writing the unique-key column vs. leaving it untouched
    // ---------------------------------------------------------------------------------------

    @Test
    public void bulkMergeWritingUniqueKeyColumn_isCapturedAsDeferredDeleteInsert() throws Exception {
        createAndEnableCdc(ACCOUNTS_DDL, ACCOUNTS_SEED, "accounts");

        start(SqlServerConnector.class, accountsConnectorConfig());
        assertConnectorIsRunning();
        consumeRecordsByTopic(ROW_COUNT);

        // A common upsert-style MERGE writes every column from its source row, including the
        // unique-key column (account_code), even when its value is unchanged. SQL Server still
        // defers this to a delete+insert because account_code is UNIQUE -- the same mechanism as
        // the plain UPDATE case above, just reached through MERGE.
        connection.execute(
                "MERGE dbo.accounts AS tgt " +
                        "USING (VALUES (1,10,500),(2,20,500),(3,30,500),(4,40,500),(5,50,500)) AS src(id,account_code,balance) " +
                        "ON tgt.id = src.id " +
                        "WHEN MATCHED THEN UPDATE SET account_code = src.account_code, balance = src.balance;");

        List<SourceRecord> topicRecords = consumeRecordsByTopic(3 * ROW_COUNT).recordsForTopic(topicName("accounts"));
        assertThat(topicRecords).hasSize(3 * ROW_COUNT);

        Map<Integer, Struct> deletesById = valuesByOperation(topicRecords, Envelope.Operation.DELETE);
        Map<Integer, Struct> insertsById = valuesByOperation(topicRecords, Envelope.Operation.CREATE);
        assertThat(deletesById).hasSize(ROW_COUNT);
        assertThat(insertsById).hasSize(ROW_COUNT);
        assertThat(valuesByOperation(topicRecords, Envelope.Operation.UPDATE)).isEmpty();

        for (int id = 1; id <= ROW_COUNT; id++) {
            assertThat(before(deletesById.get(id)).getInt32("balance")).isEqualTo(100);
            assertThat(after(insertsById.get(id)).getInt32("balance")).isEqualTo(500);
        }

        stopConnector();
    }

    @Test
    public void bulkMergeNotWritingUniqueKeyColumn_onSameTable_staysNormalUpdate() throws Exception {
        createAndEnableCdc(ACCOUNTS_DDL, ACCOUNTS_SEED, "accounts");

        start(SqlServerConnector.class, accountsConnectorConfig());
        assertConnectorIsRunning();
        consumeRecordsByTopic(ROW_COUNT);

        // Related control for the MERGE case above: same table and seed data, but this MERGE's SET
        // clause never mentions account_code. With the unique column left untouched, SQL Server
        // updates the row in place and CDC reports a normal update -- confirming it is specifically
        // *writing* the unique column, not the MERGE statement itself, that triggers deferral.
        connection.execute(
                "MERGE dbo.accounts AS tgt " +
                        "USING (VALUES (1,700),(2,700),(3,700),(4,700),(5,700)) AS src(id,balance) " +
                        "ON tgt.id = src.id " +
                        "WHEN MATCHED THEN UPDATE SET balance = src.balance;");

        List<SourceRecord> topicRecords = consumeRecordsByTopic(ROW_COUNT).recordsForTopic(topicName("accounts"));
        assertThat(topicRecords).hasSize(ROW_COUNT);

        Map<Integer, Struct> updatesById = valuesByOperation(topicRecords, Envelope.Operation.UPDATE);
        assertThat(updatesById).hasSize(ROW_COUNT);
        assertThat(valuesByOperation(topicRecords, Envelope.Operation.DELETE)).isEmpty();
        assertThat(valuesByOperation(topicRecords, Envelope.Operation.CREATE)).isEmpty();

        for (int id = 1; id <= ROW_COUNT; id++) {
            Struct value = updatesById.get(id);
            assertThat(before(value).getInt32("balance")).isEqualTo(100);
            assertThat(after(value).getInt32("balance")).isEqualTo(700);
        }

        stopConnector();
    }

    // ---------------------------------------------------------------------------------------
    // Shared helpers
    // ---------------------------------------------------------------------------------------

    private void createAndEnableCdc(String ddl, String seedInserts, String tableName) throws SQLException, InterruptedException {
        connection.execute(ddl, seedInserts);
        TestHelper.enableTableCdc(connection, tableName);
        // Gives SQL Server's capture job time to register the table before streaming starts;
        // without this pause the initial lsn_time_mapping lookup can intermittently come back empty.
        Thread.sleep(Duration.ofSeconds(TestHelper.waitTimeForLsnTimeMapping()).toMillis());
    }

    private Configuration ordersConnectorConfig() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.DATA_QUERY_MODE, DataQueryMode.DIRECT)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.orders")
                .build();
    }

    private Configuration accountsConnectorConfig() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.DATA_QUERY_MODE, DataQueryMode.DIRECT)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.accounts")
                .build();
    }

    private String topicName(String tableName) {
        return TestHelper.TEST_SERVER_NAME + "." + TestHelper.TEST_DATABASE_1 + ".dbo." + tableName;
    }

    /**
     * Groups the value structs of {@code records} by their primary key ({@code id}), keeping only
     * records whose Debezium {@code __op} matches {@code operation}. Used to assert on a bulk
     * statement's per-row before/after content regardless of the order records arrive on the topic.
     */
    private Map<Integer, Struct> valuesByOperation(List<SourceRecord> records, Envelope.Operation operation) {
        Map<Integer, Struct> result = new HashMap<>();
        for (SourceRecord record : records) {
            Struct value = (Struct) record.value();
            if (value != null && Envelope.Operation.forCode(value.getString(Envelope.FieldName.OPERATION)) == operation) {
                result.put(((Struct) record.key()).getInt32("id"), value);
            }
        }
        return result;
    }

    private Struct before(Struct value) {
        return (Struct) value.get(Envelope.FieldName.BEFORE);
    }

    private Struct after(Struct value) {
        return (Struct) value.get(Envelope.FieldName.AFTER);
    }
}
