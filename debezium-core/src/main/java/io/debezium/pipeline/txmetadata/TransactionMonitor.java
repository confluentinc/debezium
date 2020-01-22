/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.SchemaNameAdjuster;

/**
 * The class has externalized its state in {@link TransactionContext} context class so it can be stored in and recovered from offsets.
 * The class receives all processed events and keeps the transaction tracking depending on transaction id.
 * Upon transaction change the metadata events are delivered to a dedicated topic informing about {@code START/END} of the transaction,
 * including transaction id and in case of {@code END} event the amount of events generated by the transaction.
 * <p>
 * Every event seen has its {@code source} block enriched to contain
 * 
 * <ul>
 * <li>transaction id</li>
 * <li>the total event order in the transaction</li>
 * <li>the order of event per table/collection source in the transaction</li>
 * </ul>
 *
 * @author Jiri Pechanec
 */
@NotThreadSafe
public class TransactionMonitor implements DataChangeEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionMonitor.class);
    private static final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(LOGGER);

    private static final String TOPIC_PREFIX = "__debezium.transaction.";

    public static final String DEBEZIUM_TRANSACTION_KEY = "transaction";
    public static final String DEBEZIUM_TRANSACTION_ID_KEY = "id";
    public static final String DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY = "total_order";
    public static final String DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY = "data_collection_order";
    public static final String DEBEZIUM_TRANSACTION_STATUS_KEY = "status";
    public static final String DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY = "event_count";

    public static final Schema TRANSACTION_BLOCK_SCHEMA = SchemaBuilder.struct().optional()
            .field(DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
            .field(DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, Schema.INT64_SCHEMA)
            .field(DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, Schema.INT64_SCHEMA)
            .build();

    private static Schema TRANSACTION_KEY_SCHEMA = SchemaBuilder.struct()
            .name(schemaNameAdjuster.adjust("io.debezium.connector.common.TransactionMetadataKey"))
            .field(DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
            .build();
    private static Schema TRANSACTION_VALUE_SCHEMA = SchemaBuilder.struct()
            .name(schemaNameAdjuster.adjust("io.debezium.connector.common.TransactionMetadataValue"))
            .field(DEBEZIUM_TRANSACTION_STATUS_KEY, Schema.STRING_SCHEMA)
            .field(DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
            .field(DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    private final EventMetadataProvider eventMetadataProvider;
    private final String topicName;
    private final BlockingConsumer<SourceRecord> sender;
    private final CommonConnectorConfig connectorConfig;

    public TransactionMonitor(CommonConnectorConfig connectorConfig, EventMetadataProvider eventMetadataProvider, BlockingConsumer<SourceRecord> sender) {
        Objects.requireNonNull(eventMetadataProvider);
        this.topicName = TOPIC_PREFIX + connectorConfig.getLogicalName();
        this.eventMetadataProvider = eventMetadataProvider;
        this.sender = sender;
        this.connectorConfig = connectorConfig;
    }

    @Override
    public void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value) throws InterruptedException {
        if (!connectorConfig.shouldProvideTransactionMetadata()) {
            return;
        }
        final TransactionContext transactionContext = offset.getTransactionContext();

        final String txId = eventMetadataProvider.getTransactionId(source, offset, key, value);
        if (txId == null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Event '{}' has no transaction id", eventMetadataProvider.toSummaryString(source, offset, key, value));
            }
            return;
        }

        if (!transactionContext.isTransactionInProgress()) {
            transactionContext.beginTransaction(txId);
            beginTransaction(offset);
        }
        else if (!transactionContext.getTransactionId().equals(txId)) {
            endTransaction(offset);
            transactionContext.endTransaction();
            transactionContext.beginTransaction(txId);
            beginTransaction(offset);
        }
        transactionEvent(offset, source, value);
    }

    private void transactionEvent(OffsetContext offsetContext, DataCollectionId source, Struct value) {
        final long dataCollectionEventOrder = offsetContext.getTransactionContext().event(source);
        if (value == null) {
            LOGGER.debug("Event with key {} without value. Cannot enrich source block.");
            return;
        }
        final Struct sourceBlock = value.getStruct(Envelope.FieldName.SOURCE);
        if (sourceBlock == null) {
            LOGGER.debug("Event with key {} with value {} lacks source block. Cannot be enriched.");
            return;
        }
        final Struct txStruct = new Struct(TRANSACTION_BLOCK_SCHEMA);
        txStruct.put(DEBEZIUM_TRANSACTION_ID_KEY, offsetContext.getTransactionContext().getTransactionId());
        txStruct.put(DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, offsetContext.getTransactionContext().getTotalEventCount());
        txStruct.put(DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, dataCollectionEventOrder);
        sourceBlock.put(DEBEZIUM_TRANSACTION_KEY, txStruct);
    }

    private void beginTransaction(OffsetContext offsetContext) throws InterruptedException {
        final Struct key = new Struct(TRANSACTION_KEY_SCHEMA);
        key.put(DEBEZIUM_TRANSACTION_ID_KEY, offsetContext.getTransactionContext().getTransactionId());
        final Struct value = new Struct(TRANSACTION_VALUE_SCHEMA);
        value.put(DEBEZIUM_TRANSACTION_STATUS_KEY, TransactionStatus.BEGIN.name());
        value.put(DEBEZIUM_TRANSACTION_ID_KEY, offsetContext.getTransactionContext().getTransactionId());

        sender.accept(new SourceRecord(offsetContext.getPartition(), offsetContext.getOffset(),
                topicName, null, key.schema(), key, value.schema(), value));
    }

    private void endTransaction(OffsetContext offsetContext) throws InterruptedException {
        final Struct key = new Struct(TRANSACTION_KEY_SCHEMA);
        key.put(DEBEZIUM_TRANSACTION_ID_KEY, offsetContext.getTransactionContext().getTransactionId());
        final Struct value = new Struct(TRANSACTION_VALUE_SCHEMA);
        value.put(DEBEZIUM_TRANSACTION_STATUS_KEY, TransactionStatus.END.name());
        value.put(DEBEZIUM_TRANSACTION_ID_KEY, offsetContext.getTransactionContext().getTransactionId());
        value.put(DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, offsetContext.getTransactionContext().getTotalEventCount());

        sender.accept(new SourceRecord(offsetContext.getPartition(), offsetContext.getOffset(),
                topicName, null, key.schema(), key, value.schema(), value));
    }

    @Override
    public void onFilteredEvent(String event) {
    }

    @Override
    public void onErroneousEvent(String event) {
    }
}
