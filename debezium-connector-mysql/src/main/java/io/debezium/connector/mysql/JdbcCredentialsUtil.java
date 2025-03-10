package io.debezium.connector.mysql;

import io.confluent.credentialprovider.DefaultJdbcCredentials;
import io.confluent.credentialprovider.JdbcCredentials;
import io.confluent.credentialprovider.JdbcCredentialsProvider;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for handling JDBC credential providers
 */
public class JdbcCredentialsUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcCredentialsUtil.class);

    // Single cached provider instance
    private static volatile JdbcCredentialsProvider PROVIDER_INSTANCE = null;
    private static String configuredProviderClass = null;

    /**
     * Get or create a credentials provider for the given configuration
     * @param config the connector configuration
     * @return the credentials provider, or null if none configured
     */
    public static synchronized JdbcCredentialsProvider getCredentialsProvider(Configuration config) {
        String providerClass = config.getString(MySqlConnectorConfig.CREDENTIALS_PROVIDER);
        if (providerClass == null) {
            return null;
        }

        // If we already have an instance and it's the same provider class, return it
        if (PROVIDER_INSTANCE != null && providerClass.equals(configuredProviderClass)) {
            return PROVIDER_INSTANCE;
        }

        // Otherwise create a new instance
        try {
            JdbcCredentialsProvider provider = (JdbcCredentialsProvider) Class.forName(providerClass)
                    .getDeclaredConstructor().newInstance();
            provider.configure(config.asMap());

            // Cache the new instance
            PROVIDER_INSTANCE = provider;
            configuredProviderClass = providerClass;

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
     * @param provider the credentials provider
     * @param config the configuration
     * @return credentials object containing username and password
     */
    public static JdbcCredentials getCredentials(JdbcCredentialsProvider provider, Configuration config) {
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

        // Fall back to config values
        return new DefaultJdbcCredentials(
                config.getString(MySqlConnectorConfig.USER),
                config.getString(MySqlConnectorConfig.PASSWORD)
        );
    }
}