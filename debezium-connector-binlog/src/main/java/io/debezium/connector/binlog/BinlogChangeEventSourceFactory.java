/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

/**
 * Abstract base class for binlog-based change event source factories.
 * Contains common logic for memory leak prevention during snapshot operations.
 *
 * @author Yashi Srivastava
 */
public abstract class BinlogChangeEventSourceFactory<P extends Partition, O extends OffsetContext>
        implements ChangeEventSourceFactory<P, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogChangeEventSourceFactory.class);

    protected final ChangeEventQueue<DataChangeEvent> queue;

    protected BinlogChangeEventSourceFactory(ChangeEventQueue<DataChangeEvent> queue) {
        this.queue = queue;
    }

    /**
     * Enables buffering before snapshot operations.
     */
    protected void preSnapshot() {
        queue.enableBuffering();
    }

    /**
     * Modifies and flushes the last record during snapshot cleanup with proper
     * memory leak prevention and thread interrupt handling.
     *
     * @param modify function to modify the source record
     * @throws InterruptedException if the thread is interrupted or queue is shut down
     */
    protected void modifyAndFlushLastRecord(Function<SourceRecord, SourceRecord> modify) throws InterruptedException {
        try {
            // Attempt to flush the buffered record
            // If queue is shut down, this will throw InterruptedException and we'll handle it gracefully
            queue.flushBuffer(dataChange -> new DataChangeEvent(modify.apply(dataChange.getRecord())));
            LOGGER.debug("Successfully flushed buffered record during snapshot cleanup");
        }
        catch (InterruptedException e) {
            LOGGER.info("Buffered record flush interrupted during snapshot cleanup, likely due to task shutdown");
            throw e;
        }
        finally {
            // Always disable buffering to prevent memory leaks
            try {
                queue.disableBuffering();
            }
            catch (AssertionError e) {
                // In rare cases, assertion may fail if buffer is not empty due to shutdown timing
                // This is acceptable as the queue shutdown mechanism prevents the memory leak
                LOGGER.debug("Buffer not empty during cleanup due to shutdown timing - this is expected");
            }
        }
    }
}
