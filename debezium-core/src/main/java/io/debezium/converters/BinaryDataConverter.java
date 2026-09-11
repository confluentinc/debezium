/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters;

import static io.debezium.util.Loggings.maybeRedactSensitiveData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Instantiator;

/**
 * A custom value converter that allows Avro messages to be delivered as raw binary data to kafka. <p/>
 *
 * Designed to be used in the outbox pattern where payloads are pre-serialized by KafkaAvroSerializer
 * within the application, then get written as either a ByteBuffer or Byte[] to the database. <p/>
 *
 * These raw payloads become the body of the resulting Kafka message; allowing the consumer to deserialize
 * using KafkaAvroDeserializer. <p/>
 *
 * To enable the converter in a connector, the following value need to be specified:
 * "value.converter": "io.debezium.converters.BinaryDataConverter"
 *
 * @author Nathan Bradshaw
 */
public class BinaryDataConverter implements Converter, HeaderConverter, Versioned {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinaryDataConverter.class);
    private static final ConfigDef CONFIG_DEF;

    protected static final String DELEGATE_CONVERTER_TYPE = "delegate.converter.type";

    private Converter delegateConverter;

    static {
        CONFIG_DEF = ConverterConfig.newConfigDef();
        CONFIG_DEF.define(DELEGATE_CONVERTER_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Specifies the delegate converter class");
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        final String converterTypeName = (String) configs.get(DELEGATE_CONVERTER_TYPE);
        if (converterTypeName != null) {
            delegateConverter = Instantiator.getInstance(converterTypeName, null);
            // Configure the delegate under its own classloader so that all classes the delegate
            // resolves (e.g. Schema Registry serializer helpers loaded via config defaults) come
            // from a single, consistent classloader. When the delegate lives in a different plugin
            // classloader than this converter, resolving them via the thread context classloader can
            // pick up a second copy of the same class and fail with "X is not an instance of Y".
            final ClassLoader delegateClassLoader = delegateConverter.getClass().getClassLoader();
            final ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(delegateClassLoader);
                delegateConverter.configure(Configuration.from(configs).subset(DELEGATE_CONVERTER_TYPE, true).asMap(), isKey);
            }
            finally {
                Thread.currentThread().setContextClassLoader(previousClassLoader);
            }
        }
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema != null && schema.type() != Schema.Type.BYTES) {
            assertDelegateProvided(topic, value);
            LOGGER.debug("Value is not of Schema.Type.BYTES, delegating to " + delegateConverter.getClass().getName());
            return delegateConverter.fromConnectData(topic, schema, value);
        }
        else if ((value instanceof byte[])) {
            return (byte[]) value;
        }
        else if ((value instanceof ByteBuffer)) {
            return ((ByteBuffer) value).array();
        }
        else if (value != null) {
            assertDelegateProvided(topic, value);
            LOGGER.debug("Value is not of Schema.Type.BYTES, delegating to " + delegateConverter.getClass().getName());
            return delegateConverter.fromConnectData(topic, schema, value);
        }
        return null;
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value);
    }

    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return this.fromConnectData(topic, schema, value);
    }

    @Override
    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        return this.toConnectData(topic, value);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public String version() {
        return Module.version();
    }

    private void assertDelegateProvided(String name, Object type) {
        if (delegateConverter == null) {
            throw new DataException("A " + name + " of type '" + maybeRedactSensitiveData(type) + "' requires a delegate.converter.type to be configured");
        }
    }
}
