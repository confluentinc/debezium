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

public class PostgresConnectorV2ConfigDefTest extends ConfigDefinitionMetadataTest {

    public PostgresConnectorV2ConfigDefTest() {
        super(new PostgresConnector_V2());
    }

    @Test
    public void shouldSetReplicaAutoSetValidValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig_V2.REPLICA_IDENTITY_AUTOSET_VALUES, "testSchema_1.testTable_1:FULL,testSchema_2.testTable_2:DEFAULT");

        int problemCount = PostgresConnectorConfig_V2.validateReplicaAutoSetField(
                configBuilder.build(), PostgresConnectorConfig_V2.REPLICA_IDENTITY_AUTOSET_VALUES, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 0)).isTrue();
    }

    @Test
    public void shouldSetReplicaAutoSetInvalidValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig_V2.REPLICA_IDENTITY_AUTOSET_VALUES, "testSchema_1.testTable_1;FULL,testSchema_2.testTable_2;;DEFAULT");

        int problemCount = PostgresConnectorConfig_V2.validateReplicaAutoSetField(
                configBuilder.build(), PostgresConnectorConfig_V2.REPLICA_IDENTITY_AUTOSET_VALUES, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 2)).isTrue();
    }

    @Test
    public void shouldSetReplicaAutoSetRegExValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig_V2.REPLICA_IDENTITY_AUTOSET_VALUES, ".*.test.*:FULL,testSchema_2.*:DEFAULT");

        int problemCount = PostgresConnectorConfig_V2.validateReplicaAutoSetField(
                configBuilder.build(), PostgresConnectorConfig_V2.REPLICA_IDENTITY_AUTOSET_VALUES, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 0)).isTrue();
    }
}
