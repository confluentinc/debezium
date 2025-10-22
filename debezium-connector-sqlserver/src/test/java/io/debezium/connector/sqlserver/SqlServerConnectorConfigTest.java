/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

public class SqlServerConnectorConfigTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnectorConfigTest.class);

    @Test
    public void nullDatabaseNames() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig().build());
        assertFalse(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void emptyDatabaseNames() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.DATABASE_NAMES, "")
                        .build());
        assertFalse(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void nonEmptyDatabaseNames() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.DATABASE_NAMES, "testDB1")
                        .build());
        assertTrue(connectorConfig.validateAndRecord(SqlServerConnectorConfig.ALL_FIELDS, LOGGER::error));
    }

    @Test
    public void hostnameAndDefaultPortConnectionUrl() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.HOSTNAME, "example.com")
                        .build());
        assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}:${port}");
    }

    @Test
    public void hostnameAndPortConnectionUrl() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.HOSTNAME, "example.com")
                        .with(SqlServerConnectorConfig.PORT, "11433")
                        .build());
        assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}:${port}");
    }

    @Test
    public void hostnameAndInstanceConnectionUrl() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.HOSTNAME, "example.com")
                        .with(SqlServerConnectorConfig.INSTANCE, "instance")
                        .build());
        assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}\\instance");
    }

    @Test
    public void hostnameAndInstanceAndPortConnectionUrl() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.HOSTNAME, "example.com")
                        .with(SqlServerConnectorConfig.INSTANCE, "instance")
                        .with(SqlServerConnectorConfig.PORT, "11433")
                        .build());
        assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}\\instance:${port}");
    }

    @Test
    public void validQueryFetchSizeDefaults() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .build());
        assertEquals(connectorConfig.getQueryFetchSize(), 10_000);
    }

    @Test
    public void validQueryFetchSizeAvailable() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                        .with(SqlServerConnectorConfig.QUERY_FETCH_SIZE, 20_000)
                        .build());
        assertEquals(connectorConfig.getQueryFetchSize(), 20_000);
    }

    @Test
    public void testCredentialProviderFieldConfiguration() {
        // Test that the credential provider field is properly configured
        assertNotNull(SqlServerConnectorConfig.ALL_FIELDS.fieldWithName(SqlServerConnectorConfig.CREDENTIALS_PROVIDER_CLASS_NAME.name()));
        assertEquals("io.confluent.credentialproviders.DefaultJdbcCredentialsProvider",
                SqlServerConnectorConfig.CREDENTIALS_PROVIDER_CLASS_NAME.defaultValueAsString());
    }

    @Test
    public void testStaticCredentialsWithDefaultProvider() {
        // Test with default credential provider (should use static credentials)
        final SqlServerConnectorConfig config = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.DATABASE_NAMES, "testdb")
                        .with(SqlServerConnectorConfig.PASSWORD, "testpass")
                        .build());

        // Should use static credentials from configuration
        assertEquals("debezium", config.getUserName());
        assertEquals("testpass", config.getPassword());
    }

    @Test
    public void testBackwardCompatibility() {
        // Test that existing configurations without credential provider still work
        final SqlServerConnectorConfig config = new SqlServerConnectorConfig(
                defaultConfig()
                        .with(SqlServerConnectorConfig.DATABASE_NAMES, "testdb")
                        .with(SqlServerConnectorConfig.PASSWORD, "testpass")
                        .build());

        // Should work exactly as before
        assertEquals("debezium", config.getUserName());
        assertEquals("testpass", config.getPassword());
    }

    private Configuration.Builder defaultConfig() {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server")
                .with(SqlServerConnectorConfig.HOSTNAME, "localhost")
                .with(SqlServerConnectorConfig.USER, "debezium")
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                .with(KafkaSchemaHistory.TOPIC, "history");
    }

    private String connectionUrl(SqlServerConnectorConfig connectorConfig) {
        SqlServerJdbcConfiguration jdbcConfig = connectorConfig.getJdbcConfig();
        return SqlServerConnection.createUrlPattern(jdbcConfig, false);
    }
}
