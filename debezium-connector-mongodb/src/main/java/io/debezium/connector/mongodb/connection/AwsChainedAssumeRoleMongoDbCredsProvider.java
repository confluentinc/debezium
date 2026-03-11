/*
 * Copyright [2018 - 2019] Confluent Inc.
 */

package io.debezium.connector.mongodb.connection;

import io.confluent.provider.integration.aws.v2.ChainedAssumeRoleCredentialsProvider;
import io.debezium.config.Configuration;

import com.mongodb.AwsCredential;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.MongoCredential;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AWS IAM Authentication Credentials Provider for MongoDB/DocumentDB connections.
 * 
 * <p>Uses {@link ChainedAssumeRoleCredentialsProvider} to perform a two-hop STS AssumeRole
 * (Confluent middleware role -> customer role) and authenticates to MongoDB using the
 * MONGODB-AWS authentication mechanism with temporary session credentials.
 */
public class AwsChainedAssumeRoleMongoDbCredsProvider implements MongoDbAuthProvider {

  private static final Logger LOGGER =
          LoggerFactory.getLogger(AwsChainedAssumeRoleMongoDbCredsProvider.class);

  private ChainedAssumeRoleCredentialsProvider chainedCredentialsProvider;

  @Override
  public void init(Configuration config) {
    this.chainedCredentialsProvider = new ChainedAssumeRoleCredentialsProvider();
    this.chainedCredentialsProvider.configure(config.asMap());
    LOGGER.info("Initialized AWS IAM authentication provider for MongoDB");
  }

  @Override
  public Builder addAuthConfig(Builder builder) {
    Supplier<AwsCredential> awsCredentialSupplier = () -> {
      AwsCredentials creds = chainedCredentialsProvider.resolveCredentials();
      if (!(creds instanceof AwsSessionCredentials)) {
        throw new IllegalStateException(
                "Expected AwsSessionCredentials but got: " + creds.getClass().getName());
      }
      AwsSessionCredentials sessionCreds = (AwsSessionCredentials) creds;
      LOGGER.debug("Successfully resolved AWS credentials for MongoDB IAM authentication");
      return new AwsCredential(
              sessionCreds.accessKeyId(),
              sessionCreds.secretAccessKey(),
              sessionCreds.sessionToken());
    };

    MongoCredential credential = MongoCredential.createAwsCredential(null, null)
            .withMechanismProperty(MongoCredential.AWS_CREDENTIAL_PROVIDER_KEY,
                    awsCredentialSupplier);

    return builder.credential(credential);
  }
}
