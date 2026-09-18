/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

import io.debezium.doc.FixFor;

/**
 * Unit test for {@link JacksonReader}.
 *
 * @author Gunnar Morling
 */
public class JacksonReaderTest {

    @Test
    @FixFor("DBZ-657")
    public void canParseDocumentWithUnescapedControlCharacter() throws Exception {
        Document document = JacksonReader.DEFAULT_INSTANCE.read(
                // { " a CR b " : 1 2 3 }
                new String(new byte[]{ 123, 34, 97, 13, 98, 34, 58, 49, 50, 51, 125 }));

        assertThat((Object) document).isEqualTo(Document.create("a\rb", 123));
    }

    @Test
    public void parseErrorMustNotEchoSourceContent() {
        // Malformed (unterminated) JSON. The input read by this class can be signal or offset data, so the
        // parse failure below must not echo a fragment of it back via the exception location - that's what
        // JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION being disabled on the shared factory guards against.
        final String canary = "SUPER_SECRET_CANARY_VALUE_XYZ";
        final String malformedJson = "{\"a\":\"" + canary;

        try {
            JacksonReader.DEFAULT_INSTANCE.read(malformedJson);
            fail("Expected malformed JSON to fail to parse");
        }
        catch (IOException e) {
            assertThat(e.getMessage()).doesNotContain(canary);
            assertThat(e.toString()).doesNotContain(canary);
        }
    }
}
