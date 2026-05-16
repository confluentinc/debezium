/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.TestHelper;

/**
 * Integration test for {@link PostgresConnection#validateSignalDataCollection}.
 */
public class SignalDataCollectionValidateIT {

    private static final String SIGNAL_TABLE_NAME = "public.sig_validation_test";

    @Before
    public void beforeEach() {
        TestHelper.execute("DROP TABLE IF EXISTS " + SIGNAL_TABLE_NAME + ";");
    }

    @After
    public void afterEach() {
        TestHelper.execute("DROP TABLE IF EXISTS " + SIGNAL_TABLE_NAME + ";");
    }

    @Test
    public void flagOffSkipsValidationEvenWhenTableMissing() throws SQLException {
        // No table created.
        final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, SIGNAL_TABLE_NAME)
                // flag deliberately not set; default is false
                .build());

        try (PostgresConnection connection = TestHelper.create()) {
            assertThat(connection.validateSignalDataCollection(config)).isEmpty();
        }
    }

    @Test
    public void flagOnWithUnsetCollectionIsNoOp() throws SQLException {
        final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION_VALIDATION_ENABLED, true)
                .build());

        try (PostgresConnection connection = TestHelper.create()) {
            assertThat(connection.validateSignalDataCollection(config)).isEmpty();
        }
    }

    @Test
    public void missingTableProducesNotFoundError() throws SQLException {
        // No table created.
        final PostgresConnectorConfig config = validationEnabledConfig(SIGNAL_TABLE_NAME);

        try (PostgresConnection connection = TestHelper.create()) {
            final List<String> errors = connection.validateSignalDataCollection(config);
            assertThat(errors).hasSize(1);
            assertThat(errors.get(0)).contains("was not found");
        }
    }

    @Test
    public void wellFormedSignalTableProducesNoErrors() throws SQLException {
        TestHelper.execute("CREATE TABLE " + SIGNAL_TABLE_NAME
                + " (id VARCHAR(42) PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048) NULL);");
        final PostgresConnectorConfig config = validationEnabledConfig(SIGNAL_TABLE_NAME);

        try (PostgresConnection connection = TestHelper.create()) {
            assertThat(connection.validateSignalDataCollection(config)).isEmpty();
        }
    }

    @Test
    public void tooManyColumnsProducesCountError() throws SQLException {
        TestHelper.execute("CREATE TABLE " + SIGNAL_TABLE_NAME
                + " (id VARCHAR(42) PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048) NULL, ts TIMESTAMP);");
        final PostgresConnectorConfig config = validationEnabledConfig(SIGNAL_TABLE_NAME);

        try (PostgresConnection connection = TestHelper.create()) {
            final List<String> errors = connection.validateSignalDataCollection(config);
            assertThat(errors).hasSize(1);
            assertThat(errors.get(0)).contains("must have exactly 3 columns but has 4");
        }
    }

    @Test
    public void threePartSignalTableRejectedAsWrongForm() throws SQLException {
        // Runtime compares via TableId.equals on the dotted id string. Postgres's createTableId
        // drops the catalog, so events carry 2-part ids like "public.sig"; a 3-part config
        // parses to id "postgres.public.sig" which never matches. Validator surfaces the
        // mismatch and tells the user the canonical id to use.
        TestHelper.execute("CREATE TABLE " + SIGNAL_TABLE_NAME
                + " (id VARCHAR(42) PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048) NULL);");
        final String threePart = "postgres." + SIGNAL_TABLE_NAME;
        final PostgresConnectorConfig config = validationEnabledConfig(threePart);

        try (PostgresConnection connection = TestHelper.create()) {
            final List<String> errors = connection.validateSignalDataCollection(config);
            assertThat(errors).hasSize(1);
            assertThat(errors.get(0)).isEqualTo(
                    "signal.data.collection must be '" + SIGNAL_TABLE_NAME + "' (got '" + threePart + "').");
        }
    }

    private static PostgresConnectorConfig validationEnabledConfig(String signalCollection) {
        return new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, signalCollection)
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION_VALIDATION_ENABLED, true)
                .build());
    }
}
