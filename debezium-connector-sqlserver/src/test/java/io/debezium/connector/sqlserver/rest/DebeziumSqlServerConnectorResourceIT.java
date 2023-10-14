/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.rest;

import static io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure.DATABASE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;

import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import io.debezium.connector.sqlserver.Module;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.testhelper.RestExtensionTestInfrastructure;
import io.restassured.http.ContentType;


@Ignore
public class DebeziumSqlServerConnectorResourceIT {

    @BeforeClass
    public static void checkCondition() {
        Assume.assumeThat("Skipping DebeziumSqlServerConnectorResourceIT tests when assembly profile is not active!",
                System.getProperty("isAssemblyProfileActive", "false"),
                is("true"));
    }

    @Before
    public void start() throws URISyntaxException {
        RestExtensionTestInfrastructure.setupDebeziumContainer(Module.version(), DebeziumSqlServerConnectRestExtension.class.getName());
        RestExtensionTestInfrastructure.startContainers(DATABASE.SQLSERVER);
    }

    @After
    public void stop() {
        RestExtensionTestInfrastructure.stopContainers();
    }

    @Test
    public void testValidConnection() {
        ConnectorConfiguration config = getSqlServerConnectorConfiguration(1);

        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumSqlServerConnectorResource.BASE_PATH + DebeziumSqlServerConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("VALID"))
                .body("validationResults.size()", is(0));
    }

    @Test
    public void testInvalidHostnameConnection() {
        ConnectorConfiguration config = getSqlServerConnectorConfiguration(1).with(SqlServerConnectorConfig.HOSTNAME.name(), "zzzzzzzzzz");

        Locale.setDefault(new Locale("en", "US")); // to enforce errormessages in English
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body(config.toJson())
                .put(DebeziumSqlServerConnectorResource.BASE_PATH + DebeziumSqlServerConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("validationResults.size()", is(1))
                .rootPath("validationResults[0]")
                .body("property", is(SqlServerConnectorConfig.HOSTNAME.name()))
                .body("message", startsWith(
                        "Unable to connect. Check this and other connection properties. Error: The TCP/IP connection to the host zzzzzzzzzz, port 1433 has failed."));
    }

    @Test
    public void testInvalidConnection() {
        given()
                .port(RestExtensionTestInfrastructure.getDebeziumContainer().getFirstMappedPort())
                .when().contentType(ContentType.JSON).accept(ContentType.JSON).body("{\"connector.class\": \"" + SqlServerConnector.class.getName() + "\"}")
                .put(DebeziumSqlServerConnectorResource.BASE_PATH + DebeziumSqlServerConnectorResource.VALIDATE_CONNECTION_ENDPOINT)
                .then().log().all()
                .statusCode(200)
                .assertThat().body("status", equalTo("INVALID"))
                .body("validationResults.size()", is(3))
                .body("validationResults",
                        hasItems(
                                Map.of("property", SqlServerConnectorConfig.DATABASE_NAMES.name(), "message",
                                        "The 'database.names' value is invalid: Cannot be empty"),
                                Map.of("property", SqlServerConnectorConfig.TOPIC_PREFIX.name(), "message", "The 'topic.prefix' value is invalid: A value is required"),
                                Map.of("property", SqlServerConnectorConfig.HOSTNAME.name(), "message",
                                        "The 'database.hostname' value is invalid: A value is required")));
    }

    public static ConnectorConfiguration getSqlServerConnectorConfiguration(int id, String... options) {
        final ConnectorConfiguration config = ConnectorConfiguration.forJdbcContainer(RestExtensionTestInfrastructure.getSqlServerContainer())
                .with(ConnectorConfiguration.USER, "sa")
                .with(ConnectorConfiguration.PASSWORD, "Password!")
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS.name(), RestExtensionTestInfrastructure.KAFKA_HOSTNAME + ":9092")
                .with(KafkaSchemaHistory.TOPIC.name(), "dbhistory.inventory")
                .with(SqlServerConnectorConfig.DATABASE_NAMES.name(), "testDB,testDB2")
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE.name(), "initial")
                .with(SqlServerConnectorConfig.TOPIC_PREFIX.name(), "dbserver" + id)
                .with("database.encrypt", false);

        if (options != null && options.length > 0) {
            for (int i = 0; i < options.length; i += 2) {
                config.with(options[i], options[i + 1]);
            }
        }
        return config;
    }

}
