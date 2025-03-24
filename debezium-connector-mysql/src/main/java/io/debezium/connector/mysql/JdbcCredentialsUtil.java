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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.credentialproviders.DefaultJdbcCredentials;
import io.confluent.credentialproviders.JdbcCredentials;
import io.confluent.credentialproviders.JdbcCredentialsProvider;
import io.confluent.credentialproviders.aws.AwsChainedAssumeRoleRdsCredsProvider;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.AuthenticationMethod;

/**
 * Utility class for handling JDBC credential providers
 */
public class JdbcCredentialsUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcCredentialsUtil.class);

    // Single cached provider instance
    private static volatile JdbcCredentialsProvider PROVIDER_INSTANCE = null;

    /**
     * Get or create a credentials provider for the given configuration
     * @param config the connector configuration
     * @return the credentials provider, or null if none configured
     */
    public static synchronized JdbcCredentialsProvider getCredentialsProvider(Configuration config) {
        
        if (PROVIDER_INSTANCE != null) {
            return PROVIDER_INSTANCE;
        }

        AuthenticationMethod authMethod = MySqlConnectorConfig.getAuthenticationMethod(config);
        
        if (authMethod == AuthenticationMethod.IAM_ROLES) {
            return createProvider(AwsChainedAssumeRoleRdsCredsProvider.class.getName(), config);
        }
        
        String providerClass = config.getString(MySqlConnectorConfig.CREDENTIALS_PROVIDER);
        if (providerClass == null) {
            return null;
        }
        
        return createProvider(providerClass, config);
    }

    private static JdbcCredentialsProvider createProvider(String providerClass, Configuration config) {
        try {
            JdbcCredentialsProvider provider = (JdbcCredentialsProvider) Class.forName(providerClass)
                    .getDeclaredConstructor().newInstance();
            provider.configure(config.asMap());

            PROVIDER_INSTANCE = provider;

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
                config.getString(MySqlConnectorConfig.PASSWORD)
        );
    }
}
