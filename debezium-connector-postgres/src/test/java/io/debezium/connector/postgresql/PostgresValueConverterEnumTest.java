/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.sql.Types;
import java.util.Arrays;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * Unit tests for {@link PostgresValueConverter} enum handling that do not require a live database.
 * <p>
 * These tests pin the contract that {@code converter()} must recognise enum types via
 * {@link PostgresType#isEnumType()} directly, exactly as {@code schemaBuilder()} does, rather than
 * inferring enum-ness from the side-effect that the resolved JDBC id happens to be {@link Types#VARCHAR}.
 * That side-effect only holds when a type is resolved via the startup bulk load; when a type is
 * resolved via the lazy fallback path the JDBC id may come back as {@link Types#OTHER}, in which case
 * the pre-fix code returned a {@code null} element converter and threw an NPE on the first non-null
 * value (INC-12529 / CC-43614).
 */
public class PostgresValueConverterEnumTest {

    // OIDs chosen to be non-zero and outside the range of the built-in PgOid constants so that they
    // fall through to the default branch of converter()/schemaBuilder() and are resolved via the registry.
    private static final int ENUM_OID = 990001;
    private static final int ENUM_ARRAY_OID = 990002;

    private PostgresValueConverter converterWithLazilyResolvedEnum() {
        // Enum element type as it looks when resolved via the lazy fallback path: enum values are present,
        // but the JDBC id is NOT VARCHAR (this is the production-only condition that triggers the NPE).
        PostgresType enumType = mock(PostgresType.class);
        when(enumType.isEnumType()).thenReturn(true);
        when(enumType.isArrayType()).thenReturn(false);
        when(enumType.getEnumValues()).thenReturn(Arrays.asList("V1", "V2"));
        when(enumType.getOid()).thenReturn(ENUM_OID);
        when(enumType.getJdbcId()).thenReturn(Types.OTHER);
        when(enumType.getName()).thenReturn("myenum");

        PostgresType arrayType = mock(PostgresType.class);
        when(arrayType.isArrayType()).thenReturn(true);
        when(arrayType.isEnumType()).thenReturn(false);
        when(arrayType.getElementType()).thenReturn(enumType);
        when(arrayType.getOid()).thenReturn(ENUM_ARRAY_OID);
        when(arrayType.getName()).thenReturn("_myenum");

        TypeRegistry typeRegistry = mock(TypeRegistry.class);
        when(typeRegistry.get(ENUM_OID)).thenReturn(enumType);
        when(typeRegistry.get(ENUM_ARRAY_OID)).thenReturn(arrayType);

        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        return PostgresValueConverter.of(config, Charset.forName("UTF-8"), typeRegistry);
    }

    @Test
    public void shouldConvertEnumArrayWhenElementJdbcIdIsNotVarchar() {
        PostgresValueConverter converter = converterWithLazilyResolvedEnum();

        Column column = Column.editor()
                .name("value")
                .type("_myenum")
                .jdbcType(Types.ARRAY)
                .nativeType(ENUM_ARRAY_OID)
                .optional(true)
                .create();
        Schema schema = converter.schemaBuilder(column).optional().build();
        Field field = new Field("value", 0, schema);

        ValueConverter valueConverter = converter.converter(column, field);
        // Pre-fix: the element converter resolves to null and this conversion throws a NullPointerException.
        Object converted = valueConverter.convert(Arrays.asList("V1", "V2"));

        assertThat(converted).isEqualTo(Arrays.asList("V1", "V2"));
    }

    @Test
    public void shouldConvertScalarEnumWhenJdbcIdIsNotVarchar() {
        PostgresValueConverter converter = converterWithLazilyResolvedEnum();

        Column column = Column.editor()
                .name("value")
                .type("myenum")
                .jdbcType(Types.OTHER)
                .nativeType(ENUM_OID)
                .optional(true)
                .create();
        Schema schema = converter.schemaBuilder(column).optional().build();
        Field field = new Field("value", 0, schema);

        // Pre-fix: converter() returns null for a lazily-resolved enum whose JDBC id is not VARCHAR.
        ValueConverter valueConverter = converter.converter(column, field);
        assertThat(valueConverter).isNotNull();
        assertThat(valueConverter.convert("V1")).isEqualTo("V1");
    }
}
