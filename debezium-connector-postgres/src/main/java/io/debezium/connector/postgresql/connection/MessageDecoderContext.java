/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import io.debezium.connector.postgresql.PostgresConnectorConfig_V2;
import io.debezium.connector.postgresql.PostgresSchema;

/**
 * Contextual data required by {@link MessageDecoder}s.
 *
 * @author Chris Cranford
 */
public class MessageDecoderContext {

    private final PostgresConnectorConfig_V2 config;
    private final PostgresSchema schema;

    public MessageDecoderContext(PostgresConnectorConfig_V2 config, PostgresSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    public PostgresConnectorConfig_V2 getConfig() {
        return config;
    }

    public PostgresSchema getSchema() {
        return schema;
    }
}
