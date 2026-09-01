/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.DateTimeException;

import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;

/**
 * Verifies that {@link RowDeserializers} does not leak the raw column value when a temporal value fails
 * to deserialize. The underlying {@link DateTimeException} message echoes the out-of-range value (e.g.
 * "Invalid date 'APRIL 31'"), which is customer row data and must never reach the log or the thrown
 * exception.
 */
public class RowDeserializersTest {

    private static final String CANARY = "APRIL 31 CANARY_VALUE";

    @Test
    public void handleExceptionInFailModeMustNotLeakColumnValue() throws Exception {
        final Method handleException = RowDeserializers.class.getDeclaredMethod(
                "handleException", EventProcessingFailureHandlingMode.class, String.class, Exception.class, Serializable.class);
        handleException.setAccessible(true);

        final DateTimeException cause = new DateTimeException("Invalid date '" + CANARY + "'");
        try {
            handleException.invoke(null, EventProcessingFailureHandlingMode.FAIL, "date", cause, null);
            fail("FAIL mode should have rethrown");
        }
        catch (InvocationTargetException ite) {
            final Throwable thrown = ite.getCause();
            assertThat(thrown).isInstanceOf(DebeziumException.class);
            // A useful, actionable prefix (the column type) is retained for debuggability.
            assertThat(thrown.getMessage()).startsWith("Error while deserializing binlog data of date");
            // The out-of-range column value must not appear anywhere in the exception chain.
            for (Throwable t = thrown; t != null; t = t.getCause()) {
                assertThat(t.getMessage() == null ? "" : t.getMessage()).doesNotContain(CANARY);
            }
        }
    }
}
