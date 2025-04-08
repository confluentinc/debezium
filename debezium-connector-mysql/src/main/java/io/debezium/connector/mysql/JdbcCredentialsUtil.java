/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.debezium.connector.mysql;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.credentialproviders.DefaultJdbcCredentials;
import io.confluent.credentialproviders.JdbcCredentials;
import io.confluent.credentialproviders.JdbcCredentialsProvider;
import io.debezium.config.Configuration;

/**
 * Utility class for handling JDBC credential providers
 */
public class JdbcCredentialsUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcCredentialsUtil.class);

    private static final Map<String, JdbcCredentialsProvider> PROVIDER_CACHE = new ConcurrentHashMap<>();

    /**
     * Get or create a credentials provider for the given configuration
     * @param config the connector configuration
     * @return the credentials provider, or null if none configured
     */
    private static synchronized JdbcCredentialsProvider getCredentialsProvider(Configuration config) {
        String cacheKey = generateCacheKey(config);

        JdbcCredentialsProvider cachedProvider = PROVIDER_CACHE.get(cacheKey);
        if (cachedProvider != null) {
            LOGGER.debug("Returning existing credentials provider for key: {}", cacheKey);
            return cachedProvider;
        }

        String providerClass = config.getString(MySqlConnectorConfig.CREDENTIALS_PROVIDER);
        LOGGER.debug("Credentials provider class: {}", providerClass);
        if (providerClass == null) {
            LOGGER.debug("No credentials provider configured");
            return null;
        }

        JdbcCredentialsProvider provider = createProvider(providerClass, config);
        if (provider != null) {
            PROVIDER_CACHE.put(cacheKey, provider);
        }
        return provider;
    }

    private static String generateCacheKey(Configuration config) {
        // Create a unique key based on the hostname and provider integration id
        return String.format("%s:%s",
                config.getString(MySqlConnectorConfig.HOSTNAME),
                config.getString(MySqlConnectorConfig.PROVIDER_INTEGRATION_ID));
    }

    private static JdbcCredentialsProvider createProvider(String providerClass, Configuration config) {
        LOGGER.debug("Creating credentials provider: {}", providerClass);
        try {
            JdbcCredentialsProvider provider = (JdbcCredentialsProvider) Class.forName(providerClass)
                    .getDeclaredConstructor().newInstance();

            LOGGER.info("Successfully created a new instance of credentials provider of type: {}", providerClass);
            provider.configure(config.asMap());
            LOGGER.info("Configured credentials provider: {}", provider);

            return provider;
        }
        catch (Exception e) {
            LOGGER.warn("Error initializing credentials provider {}: {}", providerClass, e.getMessage());
            LOGGER.debug("Detailed provider initialization error", e);
            return null;
        }
    }

    /**
     * Get credentials from the provider or default values from config
     * @param config the configuration
     * @return credentials object containing username and password
     */
    public static JdbcCredentials getCredentials(Configuration config) {
        LOGGER.debug("Getting credentials for user '{}'", config.getString(MySqlConnectorConfig.USER));
        JdbcCredentialsProvider provider = getCredentialsProvider(config);
        if (provider != null) {
            try {
                JdbcCredentials creds = provider.getJdbcCreds();
                if (creds != null) {
                    return creds;
                }
            }
            catch (Exception e) {
                LOGGER.warn("Error getting credentials from provider: {}", e.getMessage());
                LOGGER.debug("Detailed credential retrieval error", e);
            }
        }

        return new DefaultJdbcCredentials(
                config.getString(MySqlConnectorConfig.USER),
                config.getString(MySqlConnectorConfig.PASSWORD));
    }
}
