/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.config.ConfigDefinitionMetadataTest;
import io.debezium.config.Configuration;

public class PostgresConnectorConfigDefTest extends ConfigDefinitionMetadataTest {

    public PostgresConnectorConfigDefTest() {
        super(new PostgresConnector());
    }

    @Test
    public void shouldSetReplicaAutoSetValidValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "testSchema_1.testTable_1:FULL,testSchema_2.testTable_2:DEFAULT");

        int problemCount = PostgresConnectorConfig.validateReplicaAutoSetField(
                configBuilder.build(), PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 0)).isTrue();
    }

    @Test
    public void shouldSetReplicaAutoSetInvalidValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "testSchema_1.testTable_1;FULL,testSchema_2.testTable_2;;DEFAULT");

        int problemCount = PostgresConnectorConfig.validateReplicaAutoSetField(
                configBuilder.build(), PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 2)).isTrue();
    }

    @Test
    public void shouldSetReplicaAutoSetRegExValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, ".*.test.*:FULL,testSchema_2.*:DEFAULT");

        int problemCount = PostgresConnectorConfig.validateReplicaAutoSetField(
                configBuilder.build(), PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 0)).isTrue();
    }

    @Test
    public void shouldNotValidateWalSenderTimeoutWhenNotSet() {
        // backward compatibility: when the property is absent, the connector leaves the server-side value untouched
        Configuration.Builder configBuilder = TestHelper.defaultConfig();

        int problemCount = PostgresConnectorConfig.validateWalSenderTimeout(
                configBuilder.build(), PostgresConnectorConfig.WAL_SENDER_TIMEOUT_MS, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat(problemCount).isEqualTo(0);
    }

    @Test
    public void shouldRejectWalSenderTimeoutBelowTwiceStatusInterval() {
        // 15000 < 2 * 10000 must fail
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS, 10000)
                .with(PostgresConnectorConfig.WAL_SENDER_TIMEOUT_MS, 15000);

        int problemCount = PostgresConnectorConfig.validateWalSenderTimeout(
                configBuilder.build(), PostgresConnectorConfig.WAL_SENDER_TIMEOUT_MS, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat(problemCount).isEqualTo(1);
    }

    @Test
    public void shouldAcceptWalSenderTimeoutAtLeastTwiceStatusInterval() {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS, 2000)
                .with(PostgresConnectorConfig.WAL_SENDER_TIMEOUT_MS, 50000);

        int problemCount = PostgresConnectorConfig.validateWalSenderTimeout(
                configBuilder.build(), PostgresConnectorConfig.WAL_SENDER_TIMEOUT_MS, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat(problemCount).isEqualTo(0);
    }

    @Test
    public void shouldAcceptWalSenderTimeoutAtExactlyTwiceStatusInterval() {
        // boundary: exactly 2x is allowed
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS, 5000)
                .with(PostgresConnectorConfig.WAL_SENDER_TIMEOUT_MS, 10000);

        int problemCount = PostgresConnectorConfig.validateWalSenderTimeout(
                configBuilder.build(), PostgresConnectorConfig.WAL_SENDER_TIMEOUT_MS, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat(problemCount).isEqualTo(0);
    }
}
