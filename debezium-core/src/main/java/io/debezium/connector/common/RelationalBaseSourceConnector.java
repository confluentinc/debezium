/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.ArrayList;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.util.Strings;

/**
 * Base class for Debezium's relational CDC {@link BaseSourceConnector} implementations. Provides functionality common to
 * all relational CDC connectors, such as validation.
 */
public abstract class RelationalBaseSourceConnector extends BaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalBaseSourceConnector.class);
    private static final String SERVER_ID = "database.server.id";

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        for (String  key: connectorConfigs.keySet()) {
            LOGGER.info("logging config key in validateConnection line 33");
            LOGGER.info("Config key: {} = {}", key, connectorConfigs.get(key));
        }
        Configuration config = Configuration.from(connectorConfigs);

        Map<String, String> mp = config.asMap();
        for (String  key: mp.keySet()) {
            LOGGER.info("logging config key in validate at line 36");
            LOGGER.info("Config key: {} = {}", key, mp.get(key));
        }

        // Validate all the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = validateAllFields(config);
        for (String  key: results.keySet()) {
            LOGGER.info("logging config key in validate at line 47");
            LOGGER.info("Config key: {} = {}", key, results.get(key));
        }

        if (Strings.isNullOrEmpty(config.getString(RelationalDatabaseConnectorConfig.PASSWORD))) {
            LOGGER.info("The connection password is empty");
        }

        results.values().stream()
                .filter(configValue -> !configValue.errorMessages().isEmpty())
                .forEach(configValue -> LOGGER.warn("ConfigValue '{}' has errors: {}", configValue.name(), configValue.errorMessages()));
        // Only if there are no config errors ...
        if (results.values().stream()
                .filter(
                        configValue -> !(configValue.name().equals(RelationalDatabaseConnectorConfig.TOPIC_PREFIX.name())
                                || configValue.name().equals(AbstractTopicNamingStrategy.TOPIC_HEARTBEAT_PREFIX.name())
                                || configValue.name().equals(SERVER_ID)))
                .allMatch(configValue -> configValue.errorMessages().isEmpty())) {
            // ... validate the connection too
            validateConnection(results, config);
        }
        for (String  key: results.keySet()) {
            LOGGER.info("logging config key in validate at line 65");
            LOGGER.info("Config key: {} = {}", key, results.get(key));
        }
        return new Config(new ArrayList<>(results.values()));
    }

    /**
     * Validates connection to database.
     */
    protected abstract void validateConnection(Map<String, ConfigValue> configValues, Configuration config);
}
