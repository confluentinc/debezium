/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.metadata;

import io.debezium.config.Field;
import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.PostgresConnector_V2;
import io.debezium.connector.postgresql.PostgresConnectorConfig_V2;
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;

public class PostgresConnectorMetadata implements ConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("postgres", "Debezium PostgreSQL Connector", PostgresConnector_V2.class.getName(), Module.version());
    }

    @Override
    public Field.Set getConnectorFields() {
        return PostgresConnectorConfig_V2.ALL_FIELDS;
    }

}
