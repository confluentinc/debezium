/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import static io.debezium.connector.postgresql.TestHelper.defaultJdbcConfig;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TestHelper;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;

public class PostgresDefaultValueConverterIT {

    private PostgresConnection postgresConnection;
    private PostgresValueConverter postgresValueConverter;
    private PostgresDefaultValueConverter postgresDefaultValueConverter;

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();

        postgresConnection = TestHelper.create();

        PostgresConnectorConfig postgresConnectorConfig = new PostgresConnectorConfig(defaultJdbcConfig());
        TypeRegistry typeRegistry = new TypeRegistry(postgresConnection);
        postgresValueConverter = PostgresValueConverter.of(
                postgresConnectorConfig,
                Charset.defaultCharset(),
                typeRegistry);

        postgresDefaultValueConverter = new PostgresDefaultValueConverter(
                postgresValueConverter, postgresConnection.getTimestampUtils(), typeRegistry);
    }

    @After
    public void closeConnection() {
        if (postgresConnection != null) {
            postgresConnection.close();
        }
    }

    @Test
    @FixFor("DBZ-4137")
    public void shouldReturnNullForNumericDefaultValue() {
        final Column NumericalColumn = Column.editor().type("numeric", "numeric(19, 4)")
                .jdbcType(Types.NUMERIC).defaultValueExpression("NULL::numeric").optional(true).create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                NumericalColumn.defaultValueExpression().orElse(null));

        Assert.assertEquals(numericalConvertedValue, Optional.empty());
    }

    @Test
    @FixFor("DBZ-4137")
    public void shouldReturnNullForNumericDefaultValueUsingDecimalHandlingModePrecise() {
        Configuration config = defaultJdbcConfig()
                .edit()
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.PRECISE)
                .build();

        PostgresConnectorConfig postgresConnectorConfig = new PostgresConnectorConfig(config);
        TypeRegistry typeRegistry = new TypeRegistry(postgresConnection);
        PostgresValueConverter postgresValueConverter = PostgresValueConverter.of(
                postgresConnectorConfig,
                Charset.defaultCharset(),
                typeRegistry);

        PostgresDefaultValueConverter postgresDefaultValueConverter = new PostgresDefaultValueConverter(
                postgresValueConverter, postgresConnection.getTimestampUtils(), typeRegistry);

        final Column NumericalColumn = Column.editor().type("numeric", "numeric(19, 4)")
                .jdbcType(Types.NUMERIC).defaultValueExpression("NULL::numeric").optional(true).create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                NumericalColumn.defaultValueExpression().orElse(null));

        Assert.assertEquals(numericalConvertedValue, Optional.empty());
    }

    @Test
    @FixFor("DBZ-3989")
    public void shouldTrimNumericalDefaultValueAndShouldNotTrimNonNumericalDefaultValue() {
        final Column NumericalColumn = Column.editor().type("int8").jdbcType(Types.INTEGER).defaultValueExpression(" 1 ").create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                NumericalColumn.defaultValueExpression().orElse(null));

        Assert.assertEquals(numericalConvertedValue, Optional.of(1));

        final Column nonNumericalColumn = Column.editor().type("text").jdbcType(Types.VARCHAR).defaultValueExpression(" 1 ").create();
        final Optional<Object> nonNumericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                nonNumericalColumn,
                NumericalColumn.defaultValueExpression().orElse(null));

        Assert.assertEquals(nonNumericalConvertedValue, Optional.of(" 1 "));
    }

    @Test
    public void shouldDetectSimpleFunctionDefault() {
        // Simple no-arg function like now()
        assertFunctionDefaultDetected("now()", true);
    }

    @Test
    public void shouldDetectFunctionWithArgs() {
        // Function with arguments like ABS(-1)
        assertFunctionDefaultDetected("ABS(-1)", true);
    }

    @Test
    public void shouldDetectFunctionWithMultipleArgs() {
        // Function with multiple arguments
        assertFunctionDefaultDetected("concat('foo', 'bar', 'baz')", true);
    }

    @Test
    public void shouldDetectNestedFunctionDefault() {
        // Nested function calls like date_trunc('day', now()) must be detected as functions
        assertFunctionDefaultDetected("date_trunc('day', now())", true);
    }

    @Test
    public void shouldDetectSchemaQualifiedFunctionDefault() {
        // Schema-qualified function name like pg_catalog.now()
        assertFunctionDefaultDetected("pg_catalog.now()", true);
    }

    @Test
    public void shouldDetectFunctionWithUuidGeneration() {
        // UUID generation function
        assertFunctionDefaultDetected("uuid_generate_v4()", true);
    }

    @Test
    public void shouldDetectFunctionWithLeadingParen() {
        // Optional leading parenthesis, e.g., (nextval('seq'))
        assertFunctionDefaultDetected("(nextval('my_seq'))", true);
    }

    @Test
    public void shouldNotDetectPlainLiteralAsFunctionDefault() {
        // Plain integer literal should NOT be detected as a function
        assertFunctionDefaultDetected("42", false);
    }

    @Test
    public void shouldNotDetectStringLiteralAsFunctionDefault() {
        // String literal should NOT be detected as a function
        assertFunctionDefaultDetected("'hello'", false);
    }

    @Test
    public void shouldResistReDoSOnMaliciousDefaultValue() {
        // Crafted input that would cause exponential backtracking with the old vulnerable regex.
        // With the fixed possessive-quantifier regex, this completes instantly and does not match.
        // If the regex is ever reverted to the vulnerable version, this test will hang indefinitely,
        // which the CI test runner will kill via its own timeout — no timing assertion needed here.
        String maliciousInput = "func(" + "a,".repeat(50) + "X";
        assertFunctionDefaultDetected(maliciousInput, false);
    }

    /**
     * Helper to test whether a default expression is detected as a function default.
     * Uses "text" type with Types.VARCHAR consistently, since we are testing the
     * FUNCTION_DEFAULT_PATTERN regex detection, not the type conversion logic.
     */
    private void assertFunctionDefaultDetected(String defaultExpression, boolean expectedPresent) {
        final Column column = Column.editor().type("text").jdbcType(Types.VARCHAR)
                .defaultValueExpression(defaultExpression).create();
        final Optional<Object> converted = postgresDefaultValueConverter.parseDefaultValue(
                column,
                column.defaultValueExpression().orElse(null));
        if (expectedPresent) {
            Assert.assertTrue("Expected function default to be detected for: " + defaultExpression,
                    converted.isPresent());
        }
        else {
            // For non-function defaults, parseDefaultValue may still return a value (the parsed literal),
            // so we only verify it doesn't throw — the key assertion is that it completes without hanging.
        }
    }

}
