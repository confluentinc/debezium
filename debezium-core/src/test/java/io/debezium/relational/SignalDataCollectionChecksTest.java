/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.SignalDataCollectionValidationAction;

public class SignalDataCollectionChecksTest {

    @Test
    public void happyPathReturnsNoErrors() {
        final List<Column> columns = Arrays.asList(
                column("id", Types.VARCHAR),
                column("type", Types.VARCHAR),
                column("data", Types.VARCHAR));

        assertThat(SignalDataCollectionChecks.validateShape("public.sig", columns)).isEmpty();
    }

    @Test
    public void anyColumnNamesAreAccepted() {
        // Names are intentionally not validated; the runtime reads by position and
        // the watermark INSERT writes by position.
        final List<Column> columns = Arrays.asList(
                column("signal_id", Types.VARCHAR),
                column("signal_type", Types.VARCHAR),
                column("payload", Types.VARCHAR));

        assertThat(SignalDataCollectionChecks.validateShape("public.sig", columns)).isEmpty();
    }

    @Test
    public void anyColumnTypesAreAccepted() {
        // Type validation is deferred to a later phase; today the runtime tolerates / coerces
        // for STRING-family types and only fails for hard mismatches at row-extract time.
        final List<Column> columns = Arrays.asList(
                column("id", Types.VARCHAR),
                column("type", Types.INTEGER),
                column("data", Types.BLOB));

        assertThat(SignalDataCollectionChecks.validateShape("public.sig", columns)).isEmpty();
    }

    @Test
    public void tooFewColumnsProducesCountError() {
        final List<Column> columns = Arrays.asList(
                column("id", Types.VARCHAR),
                column("type", Types.VARCHAR));

        final List<String> errors = SignalDataCollectionChecks.validateShape("public.sig", columns);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("must have exactly 3 columns but has 2");
    }

    @Test
    public void tooManyColumnsProducesCountError() {
        final List<Column> columns = Arrays.asList(
                column("id", Types.VARCHAR),
                column("type", Types.VARCHAR),
                column("data", Types.VARCHAR),
                column("ts", Types.TIMESTAMP));

        final List<String> errors = SignalDataCollectionChecks.validateShape("public.sig", columns);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("must have exactly 3 columns but has 4");
    }

    @Test
    public void rawIdIsEchoedInCountErrorMessage() {
        final List<Column> columns = Arrays.asList(
                column("a", Types.VARCHAR),
                column("b", Types.VARCHAR));

        final List<String> errors = SignalDataCollectionChecks.validateShape("my_db.my_schema.my_sig", columns);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("my_db.my_schema.my_sig");
    }

    @Test
    public void attachWithFailActionAppendsToTheSignalDataCollectionConfigValue() {
        final ConfigValue target = new ConfigValue(CommonConnectorConfig.SIGNAL_DATA_COLLECTION.name());
        final Map<String, ConfigValue> configValues = new HashMap<>();
        configValues.put(CommonConnectorConfig.SIGNAL_DATA_COLLECTION.name(), target);

        SignalDataCollectionChecks.attach(Arrays.asList("first error", "second error"), configValues,
                SignalDataCollectionValidationAction.FAIL);

        assertThat(target.errorMessages()).containsExactly("first error", "second error");
    }

    @Test
    public void attachWithWarnActionDoesNotAppendToConfigValue() {
        final ConfigValue target = new ConfigValue(CommonConnectorConfig.SIGNAL_DATA_COLLECTION.name());
        final Map<String, ConfigValue> configValues = new HashMap<>();
        configValues.put(CommonConnectorConfig.SIGNAL_DATA_COLLECTION.name(), target);

        SignalDataCollectionChecks.attach(Arrays.asList("first error", "second error"), configValues,
                SignalDataCollectionValidationAction.WARN);

        assertThat(target.errorMessages()).isEmpty();
    }

    @Test
    public void attachWithEmptyListIsNoOp() {
        final ConfigValue target = new ConfigValue(CommonConnectorConfig.SIGNAL_DATA_COLLECTION.name());
        final Map<String, ConfigValue> configValues = new HashMap<>();
        configValues.put(CommonConnectorConfig.SIGNAL_DATA_COLLECTION.name(), target);

        SignalDataCollectionChecks.attach(Collections.emptyList(), configValues, SignalDataCollectionValidationAction.FAIL);

        assertThat(target.errorMessages()).isEmpty();
    }

    private static Column column(String name, int jdbcType) {
        return Column.editor()
                .name(name)
                .jdbcType(jdbcType)
                .type(typeNameFor(jdbcType))
                .create();
    }

    private static String typeNameFor(int jdbcType) {
        switch (jdbcType) {
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.CLOB:
                return "CLOB";
            case Types.INTEGER:
                return "INTEGER";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.BLOB:
                return "BLOB";
            default:
                return "OTHER";
        }
    }
}
