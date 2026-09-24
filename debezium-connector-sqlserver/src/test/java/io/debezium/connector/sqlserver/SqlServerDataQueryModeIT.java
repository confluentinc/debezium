/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.DataQueryMode;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Testing;

/**
 * Integration tests for the two values of {@code data.query.mode}, and for the interaction between
 * {@code direct} mode and {@code streaming.fetch.size} after dbz#2012 fix.
 * <p>
 * SQL Server records a deferred update as a physical delete followed by an insert.
 * It is the one case where the two query modes see the same change differently:
 * <ul>
 * <li>{@code function} mode keeps each row's delete and insert together.</li>
 * <li>{@code direct} mode records all deletes before its inserts.</li>
 * </ul>
 * The record ordering differs between the modes and hence record ordering is not asserted.
 */
public class SqlServerDataQueryModeIT extends AbstractAsyncEngineConnectorTest {
    private static final String ID = "id";
    private static final String ORDERS = "orders";
    private static final String SHIPMENTS = "shipments";
    private static final int DEFERRED_FROM = 1;
    private static final int DEFERRED_COUNT = 5;
    private static final int PLAIN_FROM = 6;
    private static final int PLAIN_COUNT = 3;
    private static final int PK_FROM = 9;
    private static final int PK_COUNT = 2;
    private static final int PK_SHIFT = 100;
    private static final int ORDERS_SEED_ROWS = 10;
    private static final int SHIPMENTS_SEED_ROWS = 5;
    private static final int DEFERRED_RECORDS = 2 * DEFERRED_COUNT;

    // A primary key move produces a delete and an insert per row; the plain update produces one record.
    private static final int ALL_SHAPES_RECORDS = DEFERRED_RECORDS + PLAIN_COUNT + 2 * PK_COUNT;

    private static final String DEFERRED_UPDATE = "UPDATE dbo.orders SET order_ref = order_ref + '_v2' WHERE id BETWEEN 1 AND 5;";
    private static final String PLAIN_UPDATE = "UPDATE dbo.orders SET status = 'PROCESSED' WHERE id BETWEEN 6 AND 8;";
    private static final String PRIMARY_KEY_MOVE = "UPDATE dbo.orders SET id = id + " + PK_SHIFT + " WHERE id BETWEEN 9 AND 10;";
    private static final String SHIPMENTS_DEFERRED_UPDATE = "UPDATE dbo.shipments SET tracking_ref = tracking_ref + '_v2' WHERE id BETWEEN 1 AND 5;";

    private static final String ORDERS_DDL = "CREATE TABLE dbo.orders (id INT NOT NULL PRIMARY KEY, order_ref VARCHAR(50) NOT NULL, status VARCHAR(20) NOT NULL);" +
            "CREATE UNIQUE NONCLUSTERED INDEX UX_orders_order_ref ON dbo.orders(order_ref);";
    private static final String ORDERS_SEED = "INSERT INTO dbo.orders VALUES " +
            "(1,'REF-1','NEW'),(2,'REF-2','NEW'),(3,'REF-3','NEW'),(4,'REF-4','NEW'),(5,'REF-5','NEW')," +
            "(6,'REF-6','NEW'),(7,'REF-7','NEW'),(8,'REF-8','NEW')," +
            "(9,'REF-9','NEW'),(10,'REF-10','NEW');";

    private static final String SHIPMENTS_DDL = "CREATE TABLE dbo.shipments (id INT NOT NULL PRIMARY KEY, tracking_ref VARCHAR(50) NOT NULL, carrier VARCHAR(20) NOT NULL, "
            + "CONSTRAINT UQ_shipments_tracking_ref UNIQUE(tracking_ref));";
    private static final String SHIPMENTS_SEED = "INSERT INTO dbo.shipments VALUES (1,'TRK-1','ACME'),(2,'TRK-2','ACME'),(3,'TRK-3','ACME'),(4,'TRK-4','ACME'),(5,'TRK-5','ACME');";

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException, InterruptedException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        createAndEnableCdc(ORDERS_DDL, ORDERS_SEED, ORDERS);
    }

    @After
    public void after() throws SQLException {
        stopConnector();
        if (connection != null) {
            connection.close();
        }
    }

    // JUnit4 has no parameterized-test support here; each entry re-runs the case with a fresh
    // database and connection via resetFixture(), mirroring @BeforeEach/@AfterEach semantics.
    private void resetFixture() throws SQLException, InterruptedException {
        after();
        before();
    }

    @Test
    @FixFor("dbz#2012")
    public void shouldNotLoseDeferredUpdateAtAnyFetchBoundary() throws Exception {
        int[] fetchSizes = { 3, 5, 7, 12, 0 };
        for (int i = 0; i < fetchSizes.length; i++) {
            if (i > 0) {
                resetFixture();
            }
            int fetchSize = fetchSizes[i];

            start(SqlServerConnector.class, config(DataQueryMode.DIRECT, "dbo.orders", fetchSize));
            assertConnectorIsRunning();
            consumeRecordsByTopic(ORDERS_SEED_ROWS);

            connection.execute(DEFERRED_UPDATE);

            Map<Operation, Map<Integer, SourceRecord>> byOperation = groupByOperation(consumeOrders(DEFERRED_RECORDS));

            assertThat(byOperation.get(Operation.DELETE)).hasSize(DEFERRED_COUNT);
            assertThat(byOperation.get(Operation.CREATE)).hasSize(DEFERRED_COUNT);
            assertDeferredUpdate(byOperation, "REF-", "order_ref");
        }
    }

    @Test
    @FixFor("dbz#2012")
    public void shouldDeliverEveryChangeInBothQueryModes() throws Exception {
        DataQueryMode[] modes = DataQueryMode.values();
        for (int i = 0; i < modes.length; i++) {
            if (i > 0) {
                resetFixture();
            }
            DataQueryMode mode = modes[i];

            start(SqlServerConnector.class, config(mode, "dbo.orders", 3));
            assertConnectorIsRunning();
            consumeRecordsByTopic(ORDERS_SEED_ROWS);

            executeInOneTransaction(DEFERRED_UPDATE, PLAIN_UPDATE, PRIMARY_KEY_MOVE);

            Map<Operation, Map<Integer, SourceRecord>> byOperation = groupByOperation(consumeOrders(ALL_SHAPES_RECORDS));

            assertThat(byOperation.get(Operation.DELETE)).hasSize(DEFERRED_COUNT + PK_COUNT);
            assertThat(byOperation.get(Operation.CREATE)).hasSize(DEFERRED_COUNT + PK_COUNT);
            assertThat(byOperation.get(Operation.UPDATE)).hasSize(PLAIN_COUNT);

            assertDeferredUpdate(byOperation, "REF-", "order_ref");
            assertPlainUpdate(byOperation);
            assertPrimaryKeyMove(byOperation);
        }
    }

    @Test
    @FixFor("dbz#2012")
    public void shouldOrderChangesFromTwoTablesByCommandId() throws Exception {
        createAndEnableCdc(SHIPMENTS_DDL, SHIPMENTS_SEED, SHIPMENTS);

        start(SqlServerConnector.class, config(DataQueryMode.DIRECT, "dbo.orders,dbo.shipments", 3));
        assertConnectorIsRunning();
        consumeRecordsByTopic(ORDERS_SEED_ROWS + SHIPMENTS_SEED_ROWS);

        executeInOneTransaction(DEFERRED_UPDATE, SHIPMENTS_DEFERRED_UPDATE);

        SourceRecords consumed = consumeRecordsByTopic(2 * DEFERRED_RECORDS);

        assertDeferredUpdate(groupByOperation(consumed.recordsForTopic(topicName(ORDERS))), "REF-", "order_ref");
        assertDeferredUpdate(groupByOperation(consumed.recordsForTopic(topicName(SHIPMENTS))), "TRK-", "tracking_ref");
    }

    @Test
    @FixFor("dbz#2012")
    public void shouldReReadLastTransactionOnModeSwitch() throws Exception {
        DataQueryMode[][] modeSwitches = {
                { DataQueryMode.FUNCTION, DataQueryMode.DIRECT },
                { DataQueryMode.DIRECT, DataQueryMode.FUNCTION }
        };
        for (int i = 0; i < modeSwitches.length; i++) {
            if (i > 0) {
                resetFixture();
            }
            DataQueryMode from = modeSwitches[i][0];
            DataQueryMode to = modeSwitches[i][1];

            start(SqlServerConnector.class, config(from, "dbo.orders", 3));
            assertConnectorIsRunning();
            consumeRecordsByTopic(ORDERS_SEED_ROWS);

            connection.execute(DEFERRED_UPDATE);
            List<SourceRecord> records = consumeOrders(DEFERRED_RECORDS);

            assertThat(records).hasSize(DEFERRED_RECORDS);

            stopConnector();

            start(SqlServerConnector.class, config(to, "dbo.orders", 3));
            assertConnectorIsRunning();

            // The whole transaction is read again from its start, so all of it arrives a second time.
            assertDeferredUpdate(groupByOperation(consumeOrders(DEFERRED_RECORDS)), "REF-", "order_ref");
        }
    }

    @Test
    @FixFor("dbz#2012")
    public void shouldResumeCorrectlyWhenOperationsAreSkipped() throws Exception {
        Configuration config = config(DataQueryMode.DIRECT, "dbo.orders", 3)
                .edit()
                .with(SqlServerConnectorConfig.SKIPPED_OPERATIONS, "d")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        consumeRecordsByTopic(ORDERS_SEED_ROWS);

        connection.execute(DEFERRED_UPDATE);

        Map<Operation, Map<Integer, SourceRecord>> byOperation = groupByOperation(consumeOrders(DEFERRED_COUNT));

        assertThat(byOperation.get(Operation.DELETE)).isEmpty();
        assertThat(byOperation.get(Operation.CREATE)).hasSize(DEFERRED_COUNT);
        for (int id = DEFERRED_FROM; id < DEFERRED_FROM + DEFERRED_COUNT; id++) {
            assertThat(after(byOperation.get(Operation.CREATE).get(id)).getString("order_ref"))
                    .isEqualTo("REF-" + id + "_v2");
        }
    }

    @Test
    public void shouldStreamInDirectModeWhenOffsetWithoutCommandIdPointsBeyondLsnTimeMapping() throws Exception {
        final LogInterceptor errorLog = new LogInterceptor(ErrorHandler.class);

        // Stream one transaction in function mode, so the committed offset carries no command_id.
        start(SqlServerConnector.class, config(DataQueryMode.FUNCTION, "dbo.orders", 3));
        assertConnectorIsRunning();
        consumeRecordsByTopic(ORDERS_SEED_ROWS);
        connection.execute(PLAIN_UPDATE);
        assertThat(consumeOrders(PLAIN_COUNT)).hasSize(PLAIN_COUNT);
        stopConnector();

        // Simulate CDC cleanup past that transaction: nothing is left in lsn_time_mapping at or after the offset.
        purgeCdcData(ORDERS);

        // With no max LSN, the first direct-mode iteration returns before reading any change.
        start(SqlServerConnector.class, config(DataQueryMode.DIRECT, "dbo.orders", 3));
        assertConnectorIsRunning();
        TestHelper.waitForStreamingStarted();
        Thread.sleep(Duration.ofSeconds(3).toMillis());

        // A new transaction makes a max LSN available again; the later iteration must resume without failing.
        connection.execute("INSERT INTO dbo.orders VALUES (11, 'REF-11', 'NEW');");
        List<SourceRecord> records = consumeOrders(1);

        assertThat(errorLog.containsStacktraceElement("command_id must not be null in direct mode")).isFalse();
        assertThat(records).hasSize(1);
        VerifyRecord.isValidInsert(records.get(0), ID, 11);
        assertConnectorIsRunning();
    }

    @Test
    public void shouldReadWholeTransactionInDirectModeWhenCommandIdIsMissing() throws Exception {
        connection.execute(DEFERRED_UPDATE);
        TestHelper.waitForCdcRecord(connection, ORDERS, rs -> rs.getString("order_ref").equals("REF-5_v2"));

        Configuration directConfig = TestHelper.defaultConnectorConfig()
                .with(ConfigurationNames.DATABASE_CONFIG_PREFIX + JdbcConfiguration.ON_CONNECT_STATEMENTS, "USE [" + TestHelper.TEST_DATABASE_1 + "]")
                .with(SqlServerConnectorConfig.DATA_QUERY_MODE, DataQueryMode.DIRECT)
                .build();
        try (SqlServerConnection directConnection = TestHelper.testConnection(directConfig)) {
            SqlServerChangeTable changeTable = directConnection.getChangeTables(TestHelper.TEST_DATABASE_1).stream()
                    .filter(ct -> ct.getSourceTableId().table().equals(ORDERS))
                    .findFirst()
                    .orElseThrow();
            Lsn commitLsn = directConnection.getMaxTransactionLsn(TestHelper.TEST_DATABASE_1);

            List<Lsn> seqvals = new ArrayList<>();
            try (ResultSet rs = directConnection.getChangesForTable(changeTable, commitLsn, Lsn.ZERO, 0, -1, commitLsn, 0)) {
                while (rs.next()) {
                    seqvals.add(Lsn.valueOf(rs.getBytes(2)));
                }
            }
            assertThat(seqvals).hasSize(DEFERRED_RECORDS);

            // An offset without command_id resumes from a position inside the transaction; the whole transaction
            // must still be read, exactly as for command_id = -1.
            int rows = 0;
            try (ResultSet rs = directConnection.getChangesForTable(changeTable, commitLsn, seqvals.get(seqvals.size() - 1), 0, null, commitLsn, 0)) {
                while (rs.next()) {
                    rows++;
                }
            }
            assertThat(rows).isEqualTo(DEFERRED_RECORDS);
        }
    }

    private void purgeCdcData(String tableName) throws SQLException {
        connection.execute(
                "DELETE FROM cdc.[dbo_" + tableName + "_CT]",
                "DELETE FROM cdc.lsn_time_mapping");
    }

    private void assertDeferredUpdate(Map<Operation, Map<Integer, SourceRecord>> byOperation, String prefix, String column) {
        for (int id = DEFERRED_FROM; id < DEFERRED_FROM + DEFERRED_COUNT; id++) {
            SourceRecord delete = byOperation.get(Operation.DELETE).get(id);
            SourceRecord insert = byOperation.get(Operation.CREATE).get(id);

            assertThat(delete).isNotNull();
            assertThat(insert).isNotNull();
            VerifyRecord.isValidDelete(delete, ID, id);
            VerifyRecord.isValidInsert(insert, ID, id);
            assertThat(before(delete).getString(column)).isEqualTo(prefix + id);
            assertThat(after(insert).getString(column)).isEqualTo(prefix + id + "_v2");
        }
    }

    private void assertPlainUpdate(Map<Operation, Map<Integer, SourceRecord>> byOperation) {
        for (int id = PLAIN_FROM; id < PLAIN_FROM + PLAIN_COUNT; id++) {
            SourceRecord update = byOperation.get(Operation.UPDATE).get(id);
            Struct source = (Struct) update.value();

            VerifyRecord.isValidUpdate(update, ID, id);
            assertThat(before(update).getString("status")).isEqualTo("NEW");
            assertThat(after(update).getString("status")).isEqualTo("PROCESSED");
            assertThat(source.getStruct("source").getInt64("event_serial_no")).isEqualTo(2L);
        }
    }

    private void assertPrimaryKeyMove(Map<Operation, Map<Integer, SourceRecord>> byOperation) {
        for (int id = PK_FROM; id < PK_FROM + PK_COUNT; id++) {
            SourceRecord delete = byOperation.get(Operation.DELETE).get(id);
            SourceRecord insert = byOperation.get(Operation.CREATE).get(id + PK_SHIFT);

            VerifyRecord.isValidDelete(delete, ID, id);
            VerifyRecord.isValidInsert(insert, ID, id + PK_SHIFT);
            assertThat(((Struct) delete.value()).getStruct("source").getInt64("event_serial_no")).isEqualTo(1L);
            assertThat(((Struct) insert.value()).getStruct("source").getInt64("event_serial_no")).isEqualTo(1L);
        }
    }

    private Map<Operation, Map<Integer, SourceRecord>> groupByOperation(List<SourceRecord> records) {
        Map<Operation, Map<Integer, SourceRecord>> byOperation = new EnumMap<>(Operation.class);
        for (Operation operation : Operation.values()) {
            byOperation.put(operation, new HashMap<>());
        }
        if (records == null) {
            return byOperation;
        }
        for (SourceRecord record : records) {
            Struct value = (Struct) record.value();
            if (value != null) {
                Operation operation = Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
                byOperation.get(operation).put(((Struct) record.key()).getInt32(ID), record);
            }
        }
        return byOperation;
    }

    private void createAndEnableCdc(String ddl, String seedInserts, String tableName) throws SQLException, InterruptedException {
        connection.execute(ddl, seedInserts);
        TestHelper.enableTableCdc(connection, tableName);
        Thread.sleep(Duration.ofSeconds(TestHelper.waitTimeForLsnTimeMapping()).toMillis());
    }

    private void executeInOneTransaction(String... statements) throws SQLException {
        connection.setAutoCommit(false);
        for (String statement : statements) {
            connection.executeWithoutCommitting(statement);
        }
        connection.connection().commit();
        connection.setAutoCommit(true);
    }

    private Configuration config(DataQueryMode mode, String tableIncludeList, int fetchSize) {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.DATA_QUERY_MODE, mode)
                .with(SqlServerConnectorConfig.STREAMING_FETCH_SIZE, fetchSize)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                .with(SqlServerConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();
    }

    private List<SourceRecord> consumeOrders(int count) throws InterruptedException {
        return consumeRecordsByTopic(count).recordsForTopic(topicName(ORDERS));
    }

    private String topicName(String tableName) {
        return TestHelper.TEST_SERVER_NAME + "." + TestHelper.TEST_DATABASE_1 + ".dbo." + tableName;
    }

    private Struct before(SourceRecord record) {
        return (Struct) ((Struct) record.value()).get(Envelope.FieldName.BEFORE);
    }

    private Struct after(SourceRecord record) {
        return (Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER);
    }
}
