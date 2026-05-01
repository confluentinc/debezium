/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.ConditionalFail;
import io.debezium.junit.Flaky;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotWithSchemaChangesSupportTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

public class IncrementalSnapshotIT extends AbstractIncrementalSnapshotWithSchemaChangesSupportTest<SqlServerConnector> {
    private static final int POLLING_INTERVAL = 1;

    private SqlServerConnection connection;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();
    @Rule
    public ConditionalFail conditionalFail = new ConditionalFail();

    @Before
    public void before() throws SQLException, InterruptedException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE a (pk int primary key, aa int)",
                "CREATE TABLE b (pk int primary key, aa int)",
                "CREATE TABLE a42 (pk1 integer, pk2 integer, pk3 integer, pk4 integer, aa integer);",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");
        TestHelper.enableTableCdc(connection, "debezium_signal");
        TestHelper.adjustCdcPollingInterval(connection, POLLING_INTERVAL);

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        // In some cases the max lsn from lsn_time_mapping table was coming out to be null, since
        // the operations done above needed some time to be captured by the capture process.
        Thread.sleep(Duration.ofSeconds(TestHelper.waitTimeForLsnTimeMapping()).toMillis() * 2);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    protected void populateTable() throws SQLException {
        super.populateTable();
        TestHelper.enableTableCdc(connection, "a");
    }

    @Override
    protected void populateTables() throws SQLException {
        super.populateTables();
        TestHelper.enableTableCdc(connection, "a");
        TestHelper.enableTableCdc(connection, "b");
    }

    @Override
    protected Class<SqlServerConnector> connectorClass() {
        return SqlServerConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected String topicName() {
        return "server1.testDB1.dbo.a";
    }

    @Override
    protected List<String> topicNames() {
        return List.of("server1.testDB1.dbo.a", "server1.testDB1.dbo.b");
    }

    @Override
    protected String tableName() {
        return "testDB1.dbo.a";
    }

    @Override
    protected String noPKTopicName() {
        return "server1.testDB1.dbo.a42";
    }

    @Override
    protected String noPKTableName() {
        return "testDB1.dbo.a42";
    }

    @Override
    protected List<String> tableNames() {
        return List.of("testDB1.dbo.a", "testDB1.dbo.b");
    }

    @Override
    protected String tableName(String table) {
        return "testDB1.dbo." + table;
    }

    @Override
    protected String signalTableName() {
        return "dbo.debezium_signal";
    }

    @Override
    protected String alterColumnStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s %s", table, column, type);
    }

    @Override
    protected String alterColumnSetNotNullStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s %s NOT NULL", table, column, type);
    }

    @Override
    protected String alterColumnDropNotNullStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s %s NULL", table, column, type);
    }

    @Override
    protected String alterColumnSetDefaultStatement(String table, String column, String type, String defaultValue) {
        return String.format("ALTER TABLE %s ADD CONSTRAINT df_%s DEFAULT %s FOR %s", table, column, defaultValue, column);
    }

    @Override
    protected String alterColumnDropDefaultStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s DROP CONSTRAINT df_%s", table, column);
    }

    @Override
    protected void executeRenameTable(JdbcConnection connection, String newTable) throws SQLException {
        TestHelper.disableTableCdc(connection, "a");
        connection.setAutoCommit(false);
        logger.info(String.format("exec sp_rename '%s', '%s'", tableName(), "old_table"));
        connection.executeWithoutCommitting(String.format("exec sp_rename '%s', '%s'", tableName(), "old_table"));
        logger.info(String.format("exec sp_rename '%s', '%s'", tableName(newTable), "a"));
        connection.executeWithoutCommitting(String.format("exec sp_rename '%s', '%s'", tableName(newTable), "a"));
        TestHelper.enableTableCdc(connection, "a", "a", Arrays.asList("pk", "aa", "c"));
        connection.commit();
    }

    @Override
    protected String createTableStatement(String newTable, String copyTable) {
        return String.format("CREATE TABLE %s (pk int primary key, aa int)", newTable);
    }

    @Override
    protected Builder config() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, "testDB1.dbo.debezium_signal")
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 250)
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES, true)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "dbo.a42:pk1,pk2,pk3,pk4");
    }

    @Override
    protected Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        final String tableIncludeList;
        if (signalTableOnly) {
            tableIncludeList = "dbo.b";
        }
        else {
            tableIncludeList = "dbo.a,dbo.b";
        }
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, "testDB1.dbo.debezium_signal")
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 250)
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES, true)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "dbo.a42:pk1,pk2,pk3,pk4")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl);
    }

    @Override
    protected void waitForCdcTransactionPropagation(int expectedTransactions) throws Exception {
        TestHelper.waitForCdcTransactionPropagation(connection, TestHelper.TEST_DATABASE_1, expectedTransactions);
    }

    @Override
    protected String connector() {
        return "sql_server";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER_NAME;
    }

    @Override
    protected String task() {
        return "0";
    }

    @Override
    protected String database() {
        return TestHelper.TEST_DATABASE_1;
    }

    @Override
    @Flaky("DBZ-5393")
    public void stopCurrentIncrementalSnapshotWithAllCollectionsAndTakeNewNewIncrementalSnapshotAfterRestart() throws Exception {
        super.stopCurrentIncrementalSnapshotWithAllCollectionsAndTakeNewNewIncrementalSnapshotAfterRestart();
    }

    @Test
    public void snapshotRollsBackStateWhenSignalTableInsertPermissionDenied() throws Exception {
        // Reproduces the production incident where INSERT permission denied on the signal table
        // caused the incremental snapshot to get stuck as "running" permanently.
        // The fix in addDataCollectionNamesToSnapshot() should roll back the snapshot state.

        final LogInterceptor interceptor = new LogInterceptor(AbstractIncrementalSnapshotChangeEventSource.class);

        populateTable();

        // Create a restricted user that can read CDC but cannot INSERT into the signal table
        try (SqlServerConnection adminConn = TestHelper.adminConnection()) {
            adminConn.connect();
            adminConn.execute(
                    String.format("USE [%s]", TestHelper.TEST_DATABASE_1),
                    "IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = 'dbz_restricted') "
                            + "CREATE LOGIN dbz_restricted WITH PASSWORD = 'Restricted1!'",
                    "IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'dbz_restricted') "
                            + "CREATE USER dbz_restricted FOR LOGIN dbz_restricted",
                    "GRANT SELECT ON schema::dbo TO dbz_restricted",
                    "GRANT SELECT ON schema::cdc TO dbz_restricted",
                    "GRANT VIEW DATABASE STATE TO dbz_restricted",
                    "EXEC sp_addrolemember 'db_datareader', 'dbz_restricted'");
            // Explicitly deny INSERT on the signal table
            adminConn.execute("DENY INSERT ON dbo.debezium_signal TO dbz_restricted");
        }

        // Start the connector with the restricted user
        final Configuration config = config()
                .with("database.user", "dbz_restricted")
                .with("database.password", "Restricted1!")
                .build();
        start(connectorClass(), config);
        waitForConnectorToStart();
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);

        // Send the incremental snapshot signal using the admin connection (simulates customer inserting signal)
        try (SqlServerConnection adminConn = TestHelper.adminConnection()) {
            adminConn.connect();
            adminConn.execute(
                    String.format("USE [%s]", TestHelper.TEST_DATABASE_1),
                    "INSERT INTO dbo.debezium_signal (id, type, data) "
                            + "VALUES ('test-snapshot-1', 'execute-snapshot', "
                            + "'{\"data-collections\": [\"" + tableDataCollectionId() + "\"], \"type\": \"incremental\"}')");
        }

        // Wait for the signal to be processed and the error to be logged
        waitForAvailableRecords(waitTimeForRecords() * 2, TimeUnit.SECONDS);

        // Verify the error was logged and state was rolled back
        assertThat(interceptor.containsErrorMessage("Failed to read initial chunk for incremental snapshot")).isTrue();
        assertThat(interceptor.containsMessage("Incremental snapshot state rollback completed for collections")).isTrue();

        // Verify connector is still running (CDC streaming was not affected)
        assertConnectorIsRunning();

        // Now fix the permission and send a second signal -- this proves the state was actually
        // cleaned up. If the first signal left behind a stale "running" state, this second signal
        // would not trigger a new readChunk() (shouldReadChunk would be false).
        try (SqlServerConnection adminConn = TestHelper.adminConnection()) {
            adminConn.connect();
            adminConn.execute(
                    String.format("USE [%s]", TestHelper.TEST_DATABASE_1),
                    "REVOKE INSERT ON dbo.debezium_signal FROM dbz_restricted",
                    "GRANT INSERT ON dbo.debezium_signal TO dbz_restricted",
                    "GRANT DELETE ON dbo.debezium_signal TO dbz_restricted");
            adminConn.execute(
                    String.format("USE [%s]", TestHelper.TEST_DATABASE_1),
                    "INSERT INTO dbo.debezium_signal (id, type, data) "
                            + "VALUES ('test-snapshot-2', 'execute-snapshot', "
                            + "'{\"data-collections\": [\"" + tableDataCollectionId() + "\"], \"type\": \"incremental\"}')");
        }

        // The snapshot should now complete successfully
        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }

        stopConnector();
    }
}
