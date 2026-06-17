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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class KafkaLogSnapshotCoordination implements SnapshotCoordination {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLogSnapshotCoordination.class);

    private final int PARTITION_COUNT = 1;
    private final KafkaBasedLog<String, String> log;
    private final TopicAdmin topicAdmin;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ObjectMapper keyMapper = new ObjectMapper()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    private final Map<Map<String, String>, Map<String, Object>> cache = new ConcurrentHashMap<>();

    public KafkaLogSnapshotCoordination(String bootstrapServers, String topicName, String clientIdSuffix) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "snapshot-coordination-producer-" + clientIdSuffix);

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "snapshot-coordination-consumer-" + clientIdSuffix);

        Map<String, Object> adminProps = new HashMap<>();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.put(AdminClientConfig.CLIENT_ID_CONFIG, "snapshot-coordination-admin-" + clientIdSuffix);
        this.topicAdmin = new TopicAdmin(adminProps);

        createTopicIfMissing(bootstrapServers, topicName, clientIdSuffix);

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
    public void start() {
        // reads beginning to end, then starts background tailing thread
        log.start();
    }

    @Override
    public void stop() {
        try {
            log.stop();
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
        log.sendWithReceipt(keyJson, valueJson).get(30, TimeUnit.SECONDS);
        cache.put(key, new HashMap<>(data));
        LOGGER.info("Smart snapshot: wrote coordination data to key '{}', data={}", key, data);
    }

    @Override
    public Map<String, Object> readSync(Map<String, String> key) {
        try {
            log.readToEnd().get(30, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOGGER.error("Failed to write root configuration to Kafka: ", e);
            throw new ConnectException("Error writing root configuration to Kafka", e);
        }
        return cache.get(key);
    }

    @Override
    public Map<String, Object> read(Map<String, String> key) {
        return cache.get(key);
    }

    @SuppressWarnings("unchecked")
    private void onRecordConsumed(Throwable error, ConsumerRecord<String, String> record) {
        Map<String, String> key;
        try {
            key = mapper.readValue(record.key(), new TypeReference<>() {
            });
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to parse coordination key", e);
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
            throw new ConnectException("Failed to parse coordination value", e);
        }
    }

    private void createTopicIfMissing(String bootstrapServers, String topicName, String clientIdSuffix) {
        Map<String, Object> adminConfig = new HashMap<>();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminConfig.put(AdminClientConfig.CLIENT_ID_CONFIG, clientIdSuffix + "-admin");

        try (AdminClient admin = AdminClient.create(adminConfig)) {
            NewTopic topic;
            try {
                // Kafka 2.4+: omit replication factor, let the broker default apply
                topic = new NewTopic(topicName, Optional.of(PARTITION_COUNT), Optional.<Short> empty());
            }
            catch (Exception e) {
                topic = new NewTopic(topicName, PARTITION_COUNT, (short) 1);
            }
            topic.configs(Map.of(
                    TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT, // compaction = latest value per key
                    TopicConfig.SEGMENT_MS_CONFIG, "100", // optional: aggressive compaction for a short-lived coord topic
                    TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01"));

            CreateTopicsResult result = admin.createTopics(Collections.singleton(topic));
            result.all().get(30, TimeUnit.SECONDS);
            LOGGER.info("Snapshot coordination topic '{}' created", topicName);
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                LOGGER.info("Snapshot coordination topic '{}' already exists", topicName);
            }
            else {
                throw new ConnectException("Failed to create snapshot coordination topic '" + topicName + "'", e);
            }
        }
        catch (Exception e) {
            throw new ConnectException("Failed to create snapshot coordination topic '" + topicName + "'", e);
        }
    }
}
