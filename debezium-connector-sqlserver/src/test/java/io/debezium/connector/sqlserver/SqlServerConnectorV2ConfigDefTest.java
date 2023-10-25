/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.config.ConfigDefinitionMetadataTest;

public class SqlServerConnectorV2ConfigDefTest extends ConfigDefinitionMetadataTest {

    public SqlServerConnectorV2ConfigDefTest() {
        super(new SqlServerConnector_V2());
    }
}
