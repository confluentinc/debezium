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

import org.junit.Before;
import org.junit.Test;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.Tables.ColumnNameFilter;

/**
 * Unit tests for {@link SignalDataCollectionValidator} covering the config gates, the three ordered checks
 * (existence, accepted FQN shape, effective column count), the {@code column.include.list}/{@code .exclude.list}
 * interaction, and the exception-swallowing guarantee. {@link JdbcConnection} and
 * {@link RelationalDatabaseConnectorConfig} are mocked so no live database or concrete connector config is required.
 */
public class SignalDataCollectionValidatorTest {

    private static final String RAW_VALUE = "testDB.dbo.debezium_signal";
    private static final String WARN_PREFIX = "[signal.data.collection.validation]";
    private static final ColumnNameFilter MATCH_ALL = (catalog, schema, table, column) -> true;
    private static final ColumnNameFilter MATCH_NONE = (catalog, schema, table, column) -> false;

    private JdbcConnection connection;
    private RelationalDatabaseConnectorConfig connectorConfig;
    private LogInterceptor logInterceptor;

    @Before
    public void beforeEach() {
        connection = mock(JdbcConnection.class);
        connectorConfig = mock(RelationalDatabaseConnectorConfig.class);
        logInterceptor = new LogInterceptor(SignalDataCollectionValidator.class);

        when(connectorConfig.getSignalingDataCollectionId()).thenReturn(RAW_VALUE);
        when(connectorConfig.getEnabledChannels()).thenReturn(List.of("source"));
        // A streaming-capable mode by default; shouldDoNothingWhenSnapshotModeIsInitialOnly overrides this.
        when(connectorConfig.getSnapshotMode()).thenReturn(() -> "initial");
        // Mirrors RelationalDatabaseConnectorConfig's real default: no column.include.list/exclude.list configured
        // means a ColumnNameFilter that matches every column.
        when(connectorConfig.getColumnFilter()).thenReturn(MATCH_ALL);
    }

    @Test
    public void shouldDoNothingWhenSnapshotModeIsInitialOnly() throws SQLException {
        // initial_only never transitions to streaming, so the source channel never reads signal.data.collection -
        // validating it would only produce a misleading warning about a config that's never actually used.
        when(connectorConfig.getSnapshotMode()).thenReturn(() -> "initial_only");

        SignalDataCollectionValidator.validate(connection, connectorConfig);

        verifyNoInteractions(connection);
    }

    @Test
    public void shouldDoNothingWhenSignalDataCollectionIsBlank() throws SQLException {
        when(connectorConfig.getSignalingDataCollectionId()).thenReturn(" ");

        SignalDataCollectionValidator.validate(connection, connectorConfig);

        verifyNoInteractions(connection);
    }

    @Test
    public void shouldDoNothingWhenSourceChannelDisabled() throws SQLException {
        when(connectorConfig.getEnabledChannels()).thenReturn(List.of("kafka"));

        SignalDataCollectionValidator.validate(connection, connectorConfig);

        verifyNoInteractions(connection);
    }

    @Test
    public void shouldDoNothingWhenSignalDataCollectionIsValid() throws SQLException {
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidator.validate(connection, connectorConfig);

        assertThat(logInterceptor.containsWarnMessage(WARN_PREFIX)).isFalse();
    }

    @Test
    public void shouldWarnWhenColumnIncludeListReducesSignalTableToZeroColumns() throws SQLException {
        // A column.include.list that only covers other, unrelated tables (the common case in practice) matches
        // none of the signal table's columns - Debezium's real ColumnNameFilter would reduce it to zero effective
        // columns, silently breaking signaling. That must be flagged, not skipped.
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.getColumnFilter()).thenReturn(MATCH_NONE);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidator.validate(connection, connectorConfig);

        assertThat(logInterceptor.containsWarnMessage("The configured signal data collection '" + RAW_VALUE
                + "' has 0 columns but signalling requires exactly 3 columns. Please adjust the table definition in "
                + "the database or column filters using connector's column.include.list/column.exclude.list to "
                + "ensure there are exactly 3 effective columns for signal table.")).isTrue();
    }

    @Test
    public void shouldWarnWhenColumnIncludeListCoversOnlySomeOfTheRequiredColumns() throws SQLException {
        // A column.include.list that lists id/type but forgets data (a typo/oversight, not a wholesale omission)
        // must still be flagged with the effective column count actually reaching Debezium's schema.
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.getColumnFilter())
                .thenReturn((catalog, schema, table, column) -> Set.of("id", "type").contains(column));
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidator.validate(connection, connectorConfig);

        assertThat(logInterceptor.containsWarnMessage("The configured signal data collection '" + RAW_VALUE
                + "' has 2 columns but signalling requires exactly 3 columns. Please adjust the table definition in "
                + "the database or column filters using connector's column.include.list/column.exclude.list to "
                + "ensure there are exactly 3 effective columns for signal table.")).isTrue();
    }

    @Test
    public void shouldWarnWhenColumnIncludeListMatchesMoreThanTheRequiredColumns() throws SQLException {
        // A broad include-list pattern (e.g. a wildcard on the signal table) can over-include: the table has extra
        // columns beyond id/type/data, and the filter lets all of them through instead of narrowing to just 3.
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.getColumnFilter()).thenReturn(MATCH_ALL);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data", "created_at", "note"));

        SignalDataCollectionValidator.validate(connection, connectorConfig);

        assertThat(logInterceptor.containsWarnMessage("The configured signal data collection '" + RAW_VALUE
                + "' has 5 columns but signalling requires exactly 3 columns. Please adjust the table definition in "
                + "the database or column filters using connector's column.include.list/column.exclude.list to "
                + "ensure there are exactly 3 effective columns for signal table.")).isTrue();
    }

    @Test
    public void shouldDoNothingWhenColumnIncludeListCoversSignalTableColumnsDespiteExtraColumns() throws SQLException {
        // The signal table may have more than 3 physical columns; as long as the effective, filtered set is
        // exactly id/type/data, signaling works fine and must not be flagged.
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.getColumnFilter())
                .thenReturn((catalog, schema, table, column) -> Set.of("id", "type", "data").contains(column));
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data", "created_at"));

        SignalDataCollectionValidator.validate(connection, connectorConfig);

        assertThat(logInterceptor.containsWarnMessage(WARN_PREFIX)).isFalse();
    }

    @Test
    public void shouldWarnWhenTableMissing() throws SQLException {
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Collections.emptySet());

        SignalDataCollectionValidator.validate(connection, connectorConfig);

        assertThat(logInterceptor.containsWarnMessage("Signal data collection '" + RAW_VALUE
                + "' was not found in the database. Source-channel signaling will not work until this table is created.")).isTrue();
        verify(connection, never()).getColumnNames(any());
    }

    @Test
    public void shouldWarnWhenWrongShapeMatchesOneCandidate() throws SQLException {
        TableId found = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId("dbo.debezium_signal")).thenReturn(Set.of(found));
        when(connectorConfig.getSignalingDataCollectionId()).thenReturn("dbo.debezium_signal");
        when(connectorConfig.isSignalDataCollection(found)).thenReturn(false);

        SignalDataCollectionValidator.validate(connection, connectorConfig);

        assertThat(logInterceptor.containsWarnMessage(
                "signal.data.collection must be '" + found + "' (got 'dbo.debezium_signal').")).isTrue();
        verify(connection, never()).getColumnNames(any());
    }

    @Test
    public void shouldWarnWhenWrongShapeMatchesMultipleCandidates() throws SQLException {
        // A 2-part FQN can resolve to same-named tables in more than one database (e.g. SqlServer multi-db mode);
        // the message must list every candidate, sorted for determinism, instead of picking one via Set iteration order.
        TableId dbOneMatch = new TableId("db1", "dbo", "debezium_signal");
        TableId dbTwoMatch = new TableId("db2", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId("dbo.debezium_signal")).thenReturn(Set.of(dbTwoMatch, dbOneMatch));
        when(connectorConfig.getSignalingDataCollectionId()).thenReturn("dbo.debezium_signal");
        when(connectorConfig.isSignalDataCollection(dbOneMatch)).thenReturn(false);
        when(connectorConfig.isSignalDataCollection(dbTwoMatch)).thenReturn(false);

        SignalDataCollectionValidator.validate(connection, connectorConfig);

        assertThat(logInterceptor.containsWarnMessage("signal.data.collection must be one of: [" + dbOneMatch + ", " + dbTwoMatch
                + "] (got 'dbo.debezium_signal').")).isTrue();
        verify(connection, never()).getColumnNames(any());
    }

    @Test
    public void shouldWarnWhenWrongColumnCount() throws SQLException {
        TableId resolved = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data", "extra"));

        SignalDataCollectionValidator.validate(connection, connectorConfig);

        assertThat(logInterceptor.containsWarnMessage("The configured signal data collection '" + RAW_VALUE
                + "' has 4 columns but signalling requires exactly 3 columns. Please adjust the table definition in "
                + "the database or column filters using connector's column.include.list/column.exclude.list to "
                + "ensure there are exactly 3 effective columns for signal table.")).isTrue();
    }

    @Test
    public void shouldSwallowExceptionFromProbeAndNeverThrow() throws SQLException {
        when(connection.resolveSignalDataCollectionTableId(RAW_VALUE)).thenThrow(new SQLException("connection reset"));

        assertThatCode(() -> SignalDataCollectionValidator.validate(connection, connectorConfig))
                .doesNotThrowAnyException();

        assertThat(logInterceptor.containsWarnMessage(
                "Could not validate signal data collection '" + RAW_VALUE + "'")).isTrue();
    }
}
