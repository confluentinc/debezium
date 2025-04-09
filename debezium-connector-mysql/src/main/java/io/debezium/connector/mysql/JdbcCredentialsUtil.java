/*
 * Copyright [2025 - 2025] Confluent Inc.
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
    
    /**
     * Get or create a credentials provider for the given configuration
     * @param config the connector configuration
     * @return the credentials provider, or null if none configured
     */
    private static synchronized JdbcCredentialsProvider getCredentialsProvider(Configuration config) {

        String providerClass = config.getString(MySqlConnectorConfig.CREDENTIALS_PROVIDER_CLASS_NAME);
        LOGGER.debug("Credentials provider class: {}", providerClass);
        if (providerClass == null) {
            LOGGER.debug("No credentials provider configured");
            // in this case, normal username/password will be used
            return null;
        }

        JdbcCredentialsProvider provider = createProvider(providerClass, config);
        return provider;
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
    
    public static JdbcCredentialsProvider getCredentialsProviderPublic(Configuration config) {
        // return a new object
        return getCredentialsProvider(config);
    }
}
