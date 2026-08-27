/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.Before;
import org.junit.Test;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig.SignalDataCollectionValidationAction;
import io.debezium.relational.Tables.ColumnNameFilter;

/**
 * Unit tests for {@link SignalDataCollectionValidator} covering the config gates, the three ordered checks
 * (existence, accepted FQN shape, effective column count), the {@code column.include.list}/{@code .exclude.list}
 * interaction, the WARN/FAIL action split, and the exception-swallowing guarantee. {@link JdbcConnection} and
 * {@link RelationalDatabaseConnectorConfig} are mocked so no live database or concrete connector config is required.
 */
public class SignalDataCollectionValidatorTest {

    private static final String RAW_VALUE = "testDB.dbo.debezium_signal";
    private static final ColumnNameFilter MATCH_ALL = (catalog, schema, table, column) -> true;
    private static final ColumnNameFilter MATCH_NONE = (catalog, schema, table, column) -> false;

    private JdbcConnection connection;
    private RelationalDatabaseConnectorConfig connectorConfig;
    private ConfigValue signalDataCollectionValue;
    private LogInterceptor logInterceptor;

    @Before
    public void beforeEach() {
        connection = mock(JdbcConnection.class);
        connectorConfig = mock(RelationalDatabaseConnectorConfig.class);
        signalDataCollectionValue = mock(ConfigValue.class);
        logInterceptor = new LogInterceptor(SignalDataCollectionValidator.class);

        when(connectorConfig.isSignalDataCollectionValidationEnabled()).thenReturn(true);
        when(connectorConfig.getSignalingDataCollectionId()).thenReturn(RAW_VALUE);
        when(connectorConfig.getEnabledChannels()).thenReturn(List.of("source"));
        when(connectorConfig.getSignalDataCollectionValidationAction()).thenReturn(SignalDataCollectionValidationAction.WARN);
        // Mirrors RelationalDatabaseConnectorConfig's real default: no column.include.list/exclude.list configured
        // means a ColumnNameFilter that matches every column.
        when(connectorConfig.getColumnFilter()).thenReturn(MATCH_ALL);
    }

    @Test
    public void shouldDoNothingWhenValidationDisabled() throws SQLException {
        when(connectorConfig.isSignalDataCollectionValidationEnabled()).thenReturn(false);

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verifyNoInteractions(connection);
        verifyNoInteractions(signalDataCollectionValue);
    }

    @Test
    public void shouldDoNothingWhenSignalDataCollectionIsBlank() throws SQLException {
        when(connectorConfig.getSignalingDataCollectionId()).thenReturn(" ");

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verifyNoInteractions(connection);
        verifyNoInteractions(signalDataCollectionValue);
    }

    @Test
    public void shouldDoNothingWhenSourceChannelDisabled() throws SQLException {
        when(connectorConfig.getEnabledChannels()).thenReturn(List.of("kafka"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verifyNoInteractions(connection);
        verifyNoInteractions(signalDataCollectionValue);
    }

    @Test
    public void shouldDoNothingWhenSignalDataCollectionIsValid() throws SQLException {
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verifyNoInteractions(signalDataCollectionValue);
    }

    @Test
    public void shouldFailWhenColumnIncludeListReducesSignalTableToZeroColumns() throws SQLException {
        // A column.include.list that only covers other, unrelated tables (the common case in practice) matches
        // none of the signal table's columns - Debezium's real ColumnNameFilter would reduce it to zero effective
        // columns, silently breaking signaling. That must be flagged, not skipped, and the message must name
        // column.include.list specifically since that's the property actually configured.
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.isColumnsFiltered()).thenReturn(true);
        when(connectorConfig.columnIncludeList()).thenReturn("public.orders.id,public.orders.amount");
        when(connectorConfig.getColumnFilter()).thenReturn(MATCH_NONE);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(logInterceptor.containsWarnMessage("Signal data collection '" + RAW_VALUE
                + "' has 0 of its 3 columns surviving column.include.list - none are included, so every signal will be "
                + "silently dropped at runtime. Add this table's id/type/data columns explicitly to column.include.list.")).isTrue();
    }

    @Test
    public void shouldFailWhenColumnExcludeListStripsSignalTableToZeroColumns() throws SQLException {
        // Same failure mode as above but via column.exclude.list, which must be named correctly instead.
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.isColumnsFiltered()).thenReturn(true);
        when(connectorConfig.getColumnFilter()).thenReturn(MATCH_NONE);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(logInterceptor.containsWarnMessage("Signal data collection '" + RAW_VALUE
                + "' has 0 of its 3 columns surviving column.exclude.list - none are included, so every signal will be "
                + "silently dropped at runtime. Add this table's id/type/data columns explicitly to column.exclude.list.")).isTrue();
    }

    @Test
    public void shouldFailWhenColumnIncludeListCoversOnlySomeOfTheRequiredColumns() throws SQLException {
        // A column.include.list that lists id/type but forgets data (a typo/oversight, not a wholesale omission)
        // must be reported distinctly from the zero-columns case, since the fix is different (add the missing
        // column) rather than adding the table at all.
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.isColumnsFiltered()).thenReturn(true);
        when(connectorConfig.columnIncludeList()).thenReturn("public.debezium_signal.id,public.debezium_signal.type");
        when(connectorConfig.getColumnFilter())
                .thenReturn((catalog, schema, table, column) -> Set.of("id", "type").contains(column));
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(logInterceptor.containsWarnMessage("Signal data collection '" + RAW_VALUE
                + "' has 2 of 3 columns surviving column.include.list, but requires exactly 3 - verify "
                + "column.include.list covers all of this table's id/type/data columns.")).isTrue();
    }

    @Test
    public void shouldFailWhenColumnIncludeListMatchesMoreThanTheRequiredColumns() throws SQLException {
        // A broad include-list pattern (e.g. a wildcard on the signal table) can over-include: the table has extra
        // columns beyond id/type/data, and the filter lets all of them through instead of narrowing to just 3.
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.isColumnsFiltered()).thenReturn(true);
        when(connectorConfig.columnIncludeList()).thenReturn("public.debezium_signal..*");
        when(connectorConfig.getColumnFilter()).thenReturn(MATCH_ALL);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data", "created_at", "note"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(logInterceptor.containsWarnMessage("Signal data collection '" + RAW_VALUE
                + "' has 5 columns surviving column.include.list, but requires exactly 3 - narrow "
                + "column.include.list to just this table's id/type/data columns.")).isTrue();
    }

    @Test
    public void shouldDoNothingWhenColumnIncludeListCoversSignalTableColumnsDespiteExtraColumns() throws SQLException {
        // The signal table may have more than 3 physical columns; as long as the effective, filtered set is
        // exactly id/type/data, signaling works fine and must not be flagged.
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.isColumnsFiltered()).thenReturn(true);
        when(connectorConfig.getColumnFilter())
                .thenReturn((catalog, schema, table, column) -> Set.of("id", "type", "data").contains(column));
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data", "created_at"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verifyNoInteractions(signalDataCollectionValue);
    }

    @Test
    public void shouldWarnWithoutFailingConfigWhenTableMissingAndActionIsWarn() throws SQLException {
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Collections.emptySet());

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(logInterceptor.containsWarnMessage("Signal data collection '" + RAW_VALUE
                + "' was not found in the database. Source-channel signaling will not work until this table is created.")).isTrue();
        verify(signalDataCollectionValue, never()).addErrorMessage(any());
        verify(connection, never()).getColumnNames(any());
    }

    @Test
    public void shouldWarnWithoutFailingConfigWhenWrongShapeAndActionIsWarn() throws SQLException {
        TableId found = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId("dbo.debezium_signal")).thenReturn(Set.of(found));
        when(connectorConfig.getSignalingDataCollectionId()).thenReturn("dbo.debezium_signal");
        when(connectorConfig.isSignalDataCollection(found)).thenReturn(false);

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(logInterceptor.containsWarnMessage(
                "signal.data.collection must be '" + found + "' (got 'dbo.debezium_signal').")).isTrue();
        verify(signalDataCollectionValue, never()).addErrorMessage(any());
        verify(connection, never()).getColumnNames(any());
    }

    @Test
    public void shouldWarnWithoutFailingConfigWhenWrongShapeMatchesMultipleCandidates() throws SQLException {
        // A 2-part FQN can resolve to same-named tables in more than one database (e.g. SqlServer multi-db mode);
        // the message must list every candidate, sorted for determinism, instead of picking one via Set iteration order.
        TableId dbOneMatch = new TableId("db1", "dbo", "debezium_signal");
        TableId dbTwoMatch = new TableId("db2", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId("dbo.debezium_signal")).thenReturn(Set.of(dbTwoMatch, dbOneMatch));
        when(connectorConfig.getSignalingDataCollectionId()).thenReturn("dbo.debezium_signal");
        when(connectorConfig.isSignalDataCollection(dbOneMatch)).thenReturn(false);
        when(connectorConfig.isSignalDataCollection(dbTwoMatch)).thenReturn(false);

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(logInterceptor.containsWarnMessage("signal.data.collection must be one of: [" + dbOneMatch + ", " + dbTwoMatch
                + "] (got 'dbo.debezium_signal').")).isTrue();
        verify(signalDataCollectionValue, never()).addErrorMessage(any());
        verify(connection, never()).getColumnNames(any());
    }

    @Test
    public void shouldWarnWithoutFailingConfigWhenWrongColumnCountAndActionIsWarn() throws SQLException {
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data", "extra"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(logInterceptor.containsWarnMessage("Signal data collection '" + RAW_VALUE
                + "' has 4 columns but requires exactly 3 (id/type/data); no column.include.list/column.exclude.list is "
                + "configured, so all 4 columns reach Debezium's schema unfiltered.")).isTrue();
        verify(signalDataCollectionValue, never()).addErrorMessage(any());
    }

    @Test
    public void shouldDoNothingWhenValidAndActionIsFail() throws SQLException {
        when(connectorConfig.getSignalDataCollectionValidationAction()).thenReturn(SignalDataCollectionValidationAction.FAIL);
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verifyNoInteractions(signalDataCollectionValue);
    }

    @Test
    public void shouldFailConfigWhenTableMissingAndActionIsFail() throws SQLException {
        when(connectorConfig.getSignalDataCollectionValidationAction()).thenReturn(SignalDataCollectionValidationAction.FAIL);
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Collections.emptySet());

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verify(signalDataCollectionValue).addErrorMessage("Signal data collection '" + RAW_VALUE
                + "' was not found in the database. Source-channel signaling will not work until this table is created.");
    }

    @Test
    public void shouldFailConfigWhenWrongShapeAndActionIsFail() throws SQLException {
        when(connectorConfig.getSignalDataCollectionValidationAction()).thenReturn(SignalDataCollectionValidationAction.FAIL);
        when(connectorConfig.getSignalingDataCollectionId()).thenReturn("dbo.debezium_signal");
        TableId found = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId("dbo.debezium_signal")).thenReturn(Set.of(found));
        when(connectorConfig.isSignalDataCollection(found)).thenReturn(false);

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verify(signalDataCollectionValue).addErrorMessage("signal.data.collection must be '" + found + "' (got 'dbo.debezium_signal').");
    }

    @Test
    public void shouldFailConfigWhenWrongColumnCountAndActionIsFail() throws SQLException {
        when(connectorConfig.getSignalDataCollectionValidationAction()).thenReturn(SignalDataCollectionValidationAction.FAIL);
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verify(signalDataCollectionValue).addErrorMessage("Signal data collection '" + RAW_VALUE
                + "' has 2 columns but requires exactly 3 (id/type/data); no column.include.list/column.exclude.list is "
                + "configured, so all 2 columns reach Debezium's schema unfiltered.");
    }

    @Test
    public void shouldSwallowExceptionFromProbeAndNeverThrowOrFailConfig() throws SQLException {
        when(connectorConfig.getSignalDataCollectionValidationAction()).thenReturn(SignalDataCollectionValidationAction.FAIL);
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenThrow(new SQLException("connection reset"));

        assertThatCode(() -> SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue))
                .doesNotThrowAnyException();

        assertThat(logInterceptor.containsWarnMessage(
                "Could not validate signal data collection '" + RAW_VALUE + "'")).isTrue();
        verifyNoInteractions(signalDataCollectionValue);
    }
}
