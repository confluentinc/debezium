/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;

public class KafkaLogSnapshotCoordination implements SnapshotCoordination {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLogSnapshotCoordination.class);

    private static final int PARTITION_COUNT = 1;
    private static final int READ_WRITE_TIMEOUT_MS = 30_000;

    private static final String PRODUCER_OVERRIDE_PREFIX = "producer.override.";
    private static final String ADMIN_OVERRIDE_PREFIX = "admin.override.";

    private volatile boolean started = false;

    private final KafkaBasedLog<String, String> log;
    private final TopicAdmin topicAdmin;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ObjectMapper keyMapper = new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    // bootstrap + security (SASL/SSL) for the producer/consumer, from producer.override.*
    private final Map<String, Object> clientConfig;

    // bootstrap + security (SASL/SSL) for the admin client, from admin.override.* (the same overrides Kafka Connect
    // uses to create the connector's output topics)
    private final Map<String, Object> adminClientConfig;

    private final Map<Map<String, String>, Map<String, Object>> cache = new ConcurrentHashMap<>();

    private final String topicName;

    public KafkaLogSnapshotCoordination(Configuration configuration, CommonConnectorConfig commonConnectorConfig) {
        this(configuration, commonConnectorConfig, true);
    }

    public KafkaLogSnapshotCoordination(Configuration configuration, CommonConnectorConfig commonConnectorConfig, boolean createTopic) {
        // todo should this have the connector id instead? if yes how to fetch that?
        this.topicName = commonConnectorConfig.getLogicalName() + ".snapshot-coordination";
        String clientIdSuffix = commonConnectorConfig.getLogicalName() + "-coordination-connector";
        this.clientConfig = new HashMap<>(clientConfigFromOverrides(configuration, PRODUCER_OVERRIDE_PREFIX));
        // Admin client uses admin.override.* to match how Kafka Connect creates the connector's output topics.
        // TODO confirm admin.override.* (especially bootstrap.servers) is populated in Confluent Cloud for the
        // coordination admin client. producer.override.* is proven to work; if admin.override.* is not fully set,
        // switch back to the commented line below.
        this.adminClientConfig = new HashMap<>(clientConfigFromOverrides(configuration, ADMIN_OVERRIDE_PREFIX));
        // this.adminClientConfig = new HashMap<>(clientConfigFromOverrides(configuration, PRODUCER_OVERRIDE_PREFIX));
        Map<String, Object> producerProps = new HashMap<>(clientConfig);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "snapshot-coordination-producer-" + clientIdSuffix);

        Map<String, Object> consumerProps = new HashMap<>(clientConfig);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "snapshot-coordination-consumer-" + clientIdSuffix);

        Map<String, Object> adminProps = new HashMap<>(adminClientConfig);
        adminProps.put(AdminClientConfig.CLIENT_ID_CONFIG, "snapshot-coordination-admin-" + clientIdSuffix);

        // Create the topic (connector only) before allocating the long-lived TopicAdmin, so a failure here
        // can't leak that admin client.
        if (createTopic) {
            createTopicIfMissing(topicName, clientIdSuffix);
        }

        this.topicAdmin = new TopicAdmin(adminProps);
        this.log = new KafkaBasedLog<>(
                topicName, producerProps, consumerProps,
                () -> topicAdmin,
                this::onRecordConsumed,
                Time.SYSTEM,
                admin -> {
                    /* topic created externally by the Connector */ });
    }

    private String encodeKey(Map<String, String> key) throws Exception {
        return keyMapper.writeValueAsString(new TreeMap<>(key));
    }

    @Override
    public boolean startForRead() {
        // if the caller only wants to reach the coordination topic
        if (!topicExists()) {
            return false;
        }
        start();
        return true;
    }

    @Override
    public void start() {
        if (started) {
            return;
        }
        // reads beginning to end, then starts background tailing thread
        log.start();
        started = true;
    }

    @Override
    public void startRequiringTopic() {
        if (!topicExists()) {
            throw new DebeziumException("Smart snapshot: [role=coordination] Coordination topic '" + topicName
                    + "' does not exist. Tasks do not create it; the connector must provision it before tasks start.");
        }
        start();
    }

    @Override
    public void stop() {
        try {
            if (started) {
                log.stop();
            }
        }
        finally {
            topicAdmin.close();
        }
    }

    @Override
    public void write(Map<String, String> key, Map<String, Object> data) throws Exception {
        String keyJson = encodeKey(key);
        String valueJson = mapper.writeValueAsString(data);
        // synchronous
        log.sendWithReceipt(keyJson, valueJson).get(READ_WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        cache.put(key, new HashMap<>(data));
        LOGGER.debug("Smart snapshot: [role=coordination] Persisted coordination data, key={}, value={}", key, data);
    }

    /**
     * Synchronous read: catches the log up to the end before returning, so the value reflects
     * everything written to the coordination topic up to this call.
     */
    @Override
    public Map<String, Object> read(Map<String, String> key) {
        try {
            log.readToEnd().get(READ_WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DebeziumException("Interrupted while reading coordination topic", e);
        }
        catch (ExecutionException | TimeoutException e) {
            LOGGER.error("Smart snapshot: [role=coordination] Failed to read coordination topic: ", e);
            throw new DebeziumException("Error reading coordination topic", e);
        }
        return cache.get(key);
    }

    @SuppressWarnings("unchecked")
    private void onRecordConsumed(Throwable error, ConsumerRecord<String, String> record) {
        if (error != null || record == null) {
            LOGGER.error("Smart snapshot: [role=coordination] Error consuming from coordination topic '{}'", topicName, error);
            return;
        }
        Map<String, String> key;
        try {
            key = mapper.readValue(record.key(), new TypeReference<>() {
            });
        }
        catch (IOException e) {
            throw new DebeziumException("Smart snapshot: [role=coordination] Failed to parse coordination key", e);
        }
        if (record.value() == null) { // tombstone
            cache.remove(key);
            return;
        }
        try {
            Map<String, Object> data = mapper.readValue(record.value(), new TypeReference<>() {
            });
            cache.put(key, data);
        }
        catch (IOException e) {
            throw new DebeziumException("Smart snapshot: [role=coordination] Failed to parse coordination value", e);
        }
    }

    private boolean topicExists() {
        Map<String, Object> adminConfig = new HashMap<>(adminClientConfig);
        adminConfig.put(AdminClientConfig.CLIENT_ID_CONFIG, "snapshot-coordination-exists-check");
        adminConfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        adminConfig.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        try (AdminClient admin = AdminClient.create(adminConfig)) {
            admin.describeTopics(Collections.singleton(topicName)).allTopicNames().get(5, TimeUnit.SECONDS);
            return true;
        }
        catch (Exception e) {
            LOGGER.debug("Smart snapshot: [role=coordination] Coordination topic '{}' unavailable for read: {}", topicName,
                    e.toString());
            return false;
        }
    }

    /**
     * True if a coordination bootstrap is resolvable from {@code producer.override.bootstrap.servers}. That is the
     * only cluster connection a connector plugin can see (the worker-level bootstrap used for the data/internal
     * topics is not exposed to connectors), so the coordination topic reuses the same override the output producer does.
     */
    public static boolean hasBootstrap(Configuration config) {
        String fromOverride = config
                .subset(PRODUCER_OVERRIDE_PREFIX, true)
                .getString(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        return fromOverride != null && !fromOverride.isEmpty();
    }

    private void createTopicIfMissing(String topicName, String clientIdSuffix) {
        Map<String, Object> adminConfig = new HashMap<>(adminClientConfig);
        adminConfig.put(AdminClientConfig.CLIENT_ID_CONFIG, clientIdSuffix + "-admin");

        try (AdminClient admin = AdminClient.create(adminConfig)) {
            // Omit the replication factor so the broker default applies, rather than forcing an unsafe RF=1.
            NewTopic topic = new NewTopic(topicName, Optional.of(PARTITION_COUNT), Optional.<Short> empty());
            // Compaction keeps only the latest value per key, same as Kafka Connect's own connect-configs topic.
            topic.configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT));

            CreateTopicsResult result = admin.createTopics(Collections.singleton(topic));
            result.all().get(30, TimeUnit.SECONDS);
            LOGGER.info("Smart snapshot: [role=coordination] Snapshot coordination topic '{}' created", topicName);
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                LOGGER.info("Smart snapshot: [role=coordination] Snapshot coordination topic '{}' already exists", topicName);
            }
            else {
                throw new DebeziumException("Smart snapshot: [role=coordination] Failed to create snapshot coordination topic '" + topicName + "'", e);
            }
        }
        catch (Exception e) {
            throw new DebeziumException("Smart snapshot: [role=coordination] Failed to create snapshot coordination topic '" + topicName + "'", e);
        }
    }

    /**
     * Build a client config (bootstrap + security) from the given {@code *.override.*} prefix. The coordination topic
     * lives on the same cluster the connector produces records to, and these overrides are the only cluster connection
     * visible to a connector plugin, so bootstrap and credentials come from the same place Connect uses.
     */
    private Map<String, Object> clientConfigFromOverrides(Configuration config, String overridePrefix) {
        Map<String, Object> clientConfig = new HashMap<>();
        Configuration overrides = config.subset(overridePrefix, true);
        clientConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, overrides.getString(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        // carry over security/SASL/SSL props (same key names across producer/consumer/admin)
        for (String key : overrides.keys()) {
            if (key.startsWith("security.") || key.startsWith("sasl.") || key.startsWith("ssl.")) {
                clientConfig.put(key, overrides.getString(key));
            }
        }
        return clientConfig;
    }
}
