/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metadata;

import io.debezium.config.Field;
import io.debezium.connector.sqlserver.Module;
import io.debezium.connector.sqlserver.SqlServerConnector_V2;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig_V2;
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;

public class SqlServerConnectorMetadata implements ConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("sqlserver", "Debezium SQLServer Connector", SqlServerConnector_V2.class.getName(), Module.version());
    }

    @Override
    public Field.Set getConnectorFields() {
        return SqlServerConnectorConfig_V2.ALL_FIELDS;
    }
}
