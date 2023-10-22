/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.config.ConfigDefinitionMetadataTest;

public class MySqlConnectorV2ConfigDefTest extends ConfigDefinitionMetadataTest {

    public MySqlConnectorV2ConfigDefTest() {
        super(new MySqlConnector_V2());
    }
}
