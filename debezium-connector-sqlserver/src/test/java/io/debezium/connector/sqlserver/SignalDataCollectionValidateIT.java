/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.sqlserver.util.TestHelper;

/**
 * Integration test for {@link SqlServerConnection#validateSignalDataCollection}.
 */
public class SignalDataCollectionValidateIT {

    private static final String SIGNAL_TABLE = "dbo.sig_validation_test";
    private static final String FULLY_QUALIFIED = TestHelper.TEST_DATABASE_1 + "." + SIGNAL_TABLE;

    @Before
    public void beforeEach() throws SQLException {
        TestHelper.createTestDatabase();
        try (SqlServerConnection connection = TestHelper.testConnection()) {
            connection.connect();
            connection.execute("IF OBJECT_ID('" + SIGNAL_TABLE + "', 'U') IS NOT NULL DROP TABLE " + SIGNAL_TABLE);
        }
    }

    @After
    public void afterEach() throws SQLException {
        try (SqlServerConnection connection = TestHelper.testConnection()) {
            connection.connect();
            connection.execute("IF OBJECT_ID('" + SIGNAL_TABLE + "', 'U') IS NOT NULL DROP TABLE " + SIGNAL_TABLE);
        }
    }

    @Test
    public void flagOffSkipsValidationEvenWhenTableMissing() throws SQLException {
        final SqlServerConnectorConfig config = new SqlServerConnectorConfig(TestHelper.defaultConfig()
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, FULLY_QUALIFIED)
                .build());

        try (SqlServerConnection connection = TestHelper.testConnection()) {
            assertThat(connection.validateSignalDataCollection(config)).isEmpty();
        }
    }

    @Test
    public void flagOnWithUnsetCollectionIsNoOp() throws SQLException {
        final SqlServerConnectorConfig config = new SqlServerConnectorConfig(TestHelper.defaultConfig()
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION_VALIDATION_ENABLED, true)
                .build());

        try (SqlServerConnection connection = TestHelper.testConnection()) {
            assertThat(connection.validateSignalDataCollection(config)).isEmpty();
        }
    }

    @Test
    public void missingTableProducesNotFoundError() throws SQLException {
        final SqlServerConnectorConfig config = validationEnabledConfig(FULLY_QUALIFIED);

        try (SqlServerConnection connection = TestHelper.testConnection()) {
            final List<String> errors = connection.validateSignalDataCollection(config);
            assertThat(errors).hasSize(1);
            assertThat(errors.get(0)).contains("was not found");
        }
    }

    @Test
    public void wellFormedSignalTableProducesNoErrors() throws SQLException {
        try (SqlServerConnection connection = TestHelper.testConnection()) {
            connection.connect();
            connection.execute("CREATE TABLE " + SIGNAL_TABLE
                    + " (id VARCHAR(42) NOT NULL PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048) NULL)");
        }
        final SqlServerConnectorConfig config = validationEnabledConfig(FULLY_QUALIFIED);

        try (SqlServerConnection connection = TestHelper.testConnection()) {
            assertThat(connection.validateSignalDataCollection(config)).isEmpty();
        }
    }

    @Test
    public void tooManyColumnsProducesCountError() throws SQLException {
        try (SqlServerConnection connection = TestHelper.testConnection()) {
            connection.connect();
            connection.execute("CREATE TABLE " + SIGNAL_TABLE
                    + " (id VARCHAR(42) NOT NULL PRIMARY KEY, type VARCHAR(32) NOT NULL,"
                    + " data VARCHAR(2048) NULL, ts DATETIME2 NULL)");
        }
        final SqlServerConnectorConfig config = validationEnabledConfig(FULLY_QUALIFIED);

        try (SqlServerConnection connection = TestHelper.testConnection()) {
            final List<String> errors = connection.validateSignalDataCollection(config);
            assertThat(errors).hasSize(1);
            assertThat(errors.get(0)).contains("must have exactly 3 columns but has 4");
        }
    }

    @Test
    public void twoPartSignalTableIsAcceptedForCurrentDatabase() throws SQLException {
        // Customers often configure signal.data.collection as schema.table (e.g. "dbo.sig_table")
        // instead of the documented database.schema.table. Validate behavior for that form.
        try (SqlServerConnection connection = TestHelper.testConnection()) {
            connection.connect();
            connection.execute("CREATE TABLE " + SIGNAL_TABLE
                    + " (id VARCHAR(42) NOT NULL PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048) NULL)");
        }
        final SqlServerConnectorConfig config = validationEnabledConfig(SIGNAL_TABLE);

        try (SqlServerConnection connection = TestHelper.testConnection()) {
            final List<String> errors = connection.validateSignalDataCollection(config);
            assertThat(errors)
                    .as("Two-part signal.data.collection form should resolve against the connected database. "
                            + "If this assertion changes, update the validator docs and the PR body.")
                    .isEmpty();
        }
    }

    @Test
    public void wrongColumnNameProducesNameError() throws SQLException {
        try (SqlServerConnection connection = TestHelper.testConnection()) {
            connection.connect();
            connection.execute("CREATE TABLE " + SIGNAL_TABLE
                    + " (signal_id VARCHAR(42) NOT NULL PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048) NULL)");
        }
        final SqlServerConnectorConfig config = validationEnabledConfig(FULLY_QUALIFIED);

        try (SqlServerConnection connection = TestHelper.testConnection()) {
            final List<String> errors = connection.validateSignalDataCollection(config);
            assertThat(errors).hasSize(1);
            assertThat(errors.get(0)).contains("position 0").contains("'id'").contains("'signal_id'");
        }
    }

    private static SqlServerConnectorConfig validationEnabledConfig(String signalCollection) {
        return new SqlServerConnectorConfig(TestHelper.defaultConfig()
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, signalCollection)
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION_VALIDATION_ENABLED, true)
                .build());
    }
}
