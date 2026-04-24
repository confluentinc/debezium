/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.UniqueDatabase;

/**
 * Integration test for the MySQL connector's signal-data-collection validation, exercised
 * end-to-end via {@link MySqlConnector#validate}. This covers the full
 * {@code BinlogConnector.validateConnection} → {@code BinlogConnectorConnection.validateSignalDataCollection}
 * → {@code SignalDataCollectionChecks.attach} pipeline.
 */
public class MySqlSignalDataCollectionValidateIT {

    private static final String SIGNAL_TABLE_NAME = "sig_validation_test";

    private UniqueDatabase database;
    private String qualifiedSignalTable;

    @Before
    public void beforeEach() throws SQLException {
        database = new MySqlUniqueDatabase("sdc_validate", "sdc_validate_test");
        database.createAndInitialize();
        qualifiedSignalTable = database.getDatabaseName() + "." + SIGNAL_TABLE_NAME;
        dropSignalTable();
    }

    @After
    public void afterEach() throws SQLException {
        dropSignalTable();
    }

    @Test
    public void flagOffSkipsValidationEvenWhenTableMissing() {
        final Configuration config = database.defaultConfig()
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, qualifiedSignalTable)
                .build();

        assertThat(signalCollectionErrors(config)).isEmpty();
    }

    @Test
    public void flagOnWithUnsetCollectionIsNoOp() {
        final Configuration config = database.defaultConfig()
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION_VALIDATION_ENABLED, true)
                .build();

        assertThat(signalCollectionErrors(config)).isEmpty();
    }

    @Test
    public void missingTableProducesNotFoundError() {
        final Configuration config = validationEnabledConfig(qualifiedSignalTable);

        final List<String> errors = signalCollectionErrors(config);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("was not found");
    }

    @Test
    public void wellFormedSignalTableProducesNoErrors() throws SQLException {
        executeDdl("CREATE TABLE " + qualifiedSignalTable
                + " (id VARCHAR(42) PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048) NULL)");
        final Configuration config = validationEnabledConfig(qualifiedSignalTable);

        assertThat(signalCollectionErrors(config)).isEmpty();
    }

    @Test
    public void onePartSignalTableRejectedAsWrongForm() throws SQLException {
        // Runtime compares via TableId.equals on the dotted id string. MySQL events carry
        // ids like "mydb.sig"; a 1-part config parses to id "sig" which never matches.
        // Validator surfaces the mismatch and tells the user the canonical id to use.
        executeDdl("CREATE TABLE " + qualifiedSignalTable
                + " (id VARCHAR(42) PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048) NULL)");
        final Configuration config = validationEnabledConfig(SIGNAL_TABLE_NAME);

        final List<String> errors = signalCollectionErrors(config);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).isEqualTo(
                "signal.data.collection must be '" + qualifiedSignalTable + "' (got '" + SIGNAL_TABLE_NAME + "').");
    }

    @Test
    public void tooManyColumnsProducesCountError() throws SQLException {
        executeDdl("CREATE TABLE " + qualifiedSignalTable
                + " (id VARCHAR(42) PRIMARY KEY, type VARCHAR(32) NOT NULL,"
                + " data VARCHAR(2048) NULL, ts TIMESTAMP NULL)");
        final Configuration config = validationEnabledConfig(qualifiedSignalTable);

        final List<String> errors = signalCollectionErrors(config);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("must have exactly 3 columns but has 4");
    }

    @Test
    public void wrongColumnNameProducesNameError() throws SQLException {
        executeDdl("CREATE TABLE " + qualifiedSignalTable
                + " (signal_id VARCHAR(42) PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048) NULL)");
        final Configuration config = validationEnabledConfig(qualifiedSignalTable);

        final List<String> errors = signalCollectionErrors(config);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("position 0").contains("'id'").contains("'signal_id'");
    }

    private Configuration validationEnabledConfig(String signalCollection) {
        return database.defaultConfig()
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, signalCollection)
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION_VALIDATION_ENABLED, true)
                .build();
    }

    private List<String> signalCollectionErrors(Configuration config) {
        final Config validated = new MySqlConnector().validate(config.asMap());
        return validated.configValues().stream()
                .filter(cv -> CommonConnectorConfig.SIGNAL_DATA_COLLECTION.name().equals(cv.name()))
                .findFirst()
                .map(ConfigValue::errorMessages)
                .orElseThrow(() -> new AssertionError("SIGNAL_DATA_COLLECTION ConfigValue missing from validate result"));
    }

    private void executeDdl(String ddl) throws SQLException {
        try (MySqlTestConnection connection = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            connection.connect();
            connection.execute(ddl);
        }
    }

    private void dropSignalTable() throws SQLException {
        try (MySqlTestConnection connection = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            connection.connect();
            connection.execute("DROP TABLE IF EXISTS " + qualifiedSignalTable);
        }
    }
}
