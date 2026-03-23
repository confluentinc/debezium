/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.mongodb.AwsCredential;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;

import io.confluent.provider.integration.aws.v2.ChainedAssumeRoleCredentialsProvider;
import io.debezium.config.Configuration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class AwsChainedAssumeRoleMongoDbCredsProviderTest {

    // Verifies that init() creates a ChainedAssumeRoleCredentialsProvider
    // and configures it with the connector's config map.
    @Test
    public void testInitConfiguresProvider() {
        Configuration config = Configuration.from(Map.of("key1", "value1", "key2", "value2"));

        // Intercept the constructor call to verify configure() is called
        try (MockedConstruction<ChainedAssumeRoleCredentialsProvider> mocked =
                     Mockito.mockConstruction(ChainedAssumeRoleCredentialsProvider.class)) {

            AwsChainedAssumeRoleMongoDbCredsProvider provider = new AwsChainedAssumeRoleMongoDbCredsProvider();
            provider.init(config);

            assertThat(mocked.constructed()).hasSize(1);
            ChainedAssumeRoleCredentialsProvider constructed = mocked.constructed().get(0);
            verify(constructed).configure(config.asMap());
        }
    }

    // Verifies that addAuthConfig() sets MONGODB-AWS mechanism on the builder
    // and wires a credential supplier that correctly maps AwsSessionCredentials
    // to MongoDB's AwsCredential (accessKeyId, secretAccessKey, sessionToken).
    @Test
    public void testAddAuthConfigSetsAwsCredential() throws Exception {
        AwsChainedAssumeRoleMongoDbCredsProvider provider = new AwsChainedAssumeRoleMongoDbCredsProvider();

        // Mock the chained provider to return known session credentials
        ChainedAssumeRoleCredentialsProvider mockChained = mock(ChainedAssumeRoleCredentialsProvider.class);
        AwsSessionCredentials sessionCredentials = AwsSessionCredentials.builder()
                .accessKeyId("AKID")
                .secretAccessKey("secret")
                .sessionToken("token")
                .build();
        when(mockChained.resolveCredentials()).thenReturn(sessionCredentials);

        setField(provider, "chainedCredentialsProvider", mockChained);

        MongoClientSettings.Builder builder = MongoClientSettings.builder();
        MongoClientSettings.Builder result = provider.addAuthConfig(builder);

        MongoClientSettings settings = result.build();
        MongoCredential credential = settings.getCredential();

        assertThat(credential).isNotNull();
        assertThat(credential.getMechanism()).isEqualTo("MONGODB-AWS");

        // Extract the supplier from mechanism properties and invoke it
        Supplier<AwsCredential> supplier = credential.getMechanismProperty(
                MongoCredential.AWS_CREDENTIAL_PROVIDER_KEY, null);
        assertThat(supplier).isNotNull();

        AwsCredential awsCredential = supplier.get();
        assertThat(awsCredential.getAccessKeyId()).isEqualTo("AKID");
        assertThat(awsCredential.getSecretAccessKey()).isEqualTo("secret");
        assertThat(awsCredential.getSessionToken()).isEqualTo("token");
    }

    // Verifies that the credential supplier throws IllegalStateException
    // when the chained provider returns basic credentials instead of session credentials.
    // This guards against misconfigured IAM roles that don't use STS AssumeRole.
    @Test
    public void testAddAuthConfigThrowsForNonSessionCredentials() throws Exception {
        AwsChainedAssumeRoleMongoDbCredsProvider provider = new AwsChainedAssumeRoleMongoDbCredsProvider();

        // Return basic credentials (no session token) to trigger the guard
        ChainedAssumeRoleCredentialsProvider mockChained = mock(ChainedAssumeRoleCredentialsProvider.class);
        when(mockChained.resolveCredentials()).thenReturn(
                AwsBasicCredentials.create("AKID", "secret"));

        setField(provider, "chainedCredentialsProvider", mockChained);

        MongoClientSettings.Builder builder = MongoClientSettings.builder();
        MongoClientSettings.Builder result = provider.addAuthConfig(builder);

        MongoClientSettings settings = result.build();
        MongoCredential credential = settings.getCredential();

        Supplier<AwsCredential> supplier = credential.getMechanismProperty(
                MongoCredential.AWS_CREDENTIAL_PROVIDER_KEY, null);
        assertThat(supplier).isNotNull();

        assertThatThrownBy(supplier::get)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Expected AwsSessionCredentials but got");
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
