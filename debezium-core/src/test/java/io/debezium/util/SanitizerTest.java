/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.Test;

public class SanitizerTest {

    @Test
    public void shouldLeaveJmxSafeValuesUnquoted() {
        assertThat(Sanitizer.jmxSanitize("my-connector.0"), is("my-connector.0"));
        assertThat(Sanitizer.jmxSanitize("a b_c-1.2%"), is("a b_c-1.2%"));
        assertThat(Sanitizer.jmxSanitize(""), is(""));
    }

    @Test
    public void shouldQuoteValuesWithJmxUnsafeCharacters() {
        assertThat(Sanitizer.jmxSanitize("has:colon"), is(ObjectName.quote("has:colon")));
        assertThat(Sanitizer.jmxSanitize("a*b,c=d"), is(ObjectName.quote("a*b,c=d")));
    }

    @Test
    public void shouldProduceValueUsableInObjectName() throws MalformedObjectNameException {
        String unsafeValue = "a*b,c=d:e\"f";
        ObjectName objectName = new ObjectName("debezium.test:type=connector-metrics,tag=" + Sanitizer.jmxSanitize(unsafeValue));

        assertThat(objectName.getKeyProperty("tag"), is(ObjectName.quote(unsafeValue)));
    }

    @Test
    public void shouldThrowNullPointerExceptionForNullValue() {
        assertThrows(NullPointerException.class, () -> Sanitizer.jmxSanitize(null));
    }
}
