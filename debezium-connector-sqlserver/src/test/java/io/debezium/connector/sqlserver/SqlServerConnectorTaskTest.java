package io.debezium.connector.sqlserver;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.function.LogPositionValidator;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.HistorizedDatabaseSchema;
import io.debezium.spi.snapshot.Snapshotter;

/**
 * Unit test for SqlServerConnectorTask validateSchemaHistory method
 */
@RunWith(MockitoJUnitRunner.class)
public class SqlServerConnectorTaskTest {

    @Mock
    private CommonConnectorConfig mockConnectorConfig;

    @Mock
    private LogPositionValidator mockLogPositionValidator;

    @Mock
    private HistorizedDatabaseSchema mockHistorizedSchema;

    @Mock
    private DatabaseSchema mockNonHistorizedSchema;

    @Mock
    private Snapshotter mockSnapshotter;

    @Mock
    private SqlServerPartition mockPartition;

    @Mock
    private SqlServerOffsetContext mockOffsetContext;

    // Use the existing test pattern from BaseSourceTaskSnapshotModesValidationTest
    private final SqlServerTestTask task = new SqlServerTestTask();

    @Test
    public void testValidateSchemaHistoryWithFirstTimeConnectorStart() {
        // Test scenario: Connector started for the first time (no previous offsets)
        Offsets<SqlServerPartition, SqlServerOffsetContext> offsets = Offsets.of(mockPartition, null);

        when(mockHistorizedSchema.isHistorized()).thenReturn(true);
        when(mockSnapshotter.shouldSnapshotOnSchemaError()).thenReturn(false);

        // Should not throw any exception and should initialize storage
        task.testValidateSchemaHistory(mockConnectorConfig, mockLogPositionValidator, offsets, mockHistorizedSchema, mockSnapshotter);

        verify(mockHistorizedSchema).initializeStorage();
    }

    @Test
    public void testValidateSchemaHistoryWithNonHistorizedSchema() {
        // Test scenario: Non-historized schema (should not initialize storage)
        Offsets<SqlServerPartition, SqlServerOffsetContext> offsets = Offsets.of(mockPartition, null);

        when(mockNonHistorizedSchema.isHistorized()).thenReturn(false);
        when(mockSnapshotter.shouldSnapshotOnSchemaError()).thenReturn(false);

        // Should not throw any exception
        task.testValidateSchemaHistory(mockConnectorConfig, mockLogPositionValidator, offsets, mockNonHistorizedSchema, mockSnapshotter);

        // Non-historized schema doesn't have initializeStorage method
        verify(mockHistorizedSchema, times(0)).initializeStorage();
    }

    @Test(expected = DebeziumException.class)
    public void testValidateSchemaHistoryThrowsExceptionWhenSnapshotRunningButSnapshotsDisabled() {
        // Test scenario: Previous snapshot was incomplete but snapshots are now disabled
        Offsets<SqlServerPartition, SqlServerOffsetContext> offsets = Offsets.of(mockPartition, mockOffsetContext);

        when(mockOffsetContext.isInitialSnapshotRunning()).thenReturn(true);
        when(mockSnapshotter.shouldSnapshotData(true, true)).thenReturn(false);
        when(mockSnapshotter.shouldSnapshotSchema(true, true)).thenReturn(false);

        // Should throw DebeziumException
        task.testValidateSchemaHistory(mockConnectorConfig, mockLogPositionValidator, offsets, mockHistorizedSchema, mockSnapshotter);
    }

    @Test
    public void testValidateSchemaHistoryAllowsSnapshotWhenPreviousSnapshotIncomplete() {
        // Test scenario: Previous snapshot was incomplete but snapshots are allowed
        Offsets<SqlServerPartition, SqlServerOffsetContext> offsets = Offsets.of(mockPartition, mockOffsetContext);

        when(mockOffsetContext.isInitialSnapshotRunning()).thenReturn(true);
        when(mockSnapshotter.shouldSnapshotData(true, true)).thenReturn(true);

        // Should not throw any exception
        task.testValidateSchemaHistory(mockConnectorConfig, mockLogPositionValidator, offsets, mockHistorizedSchema, mockSnapshotter);
    }

    @Test(expected = DebeziumException.class)
    public void testValidateSchemaHistoryThrowsExceptionWhenHistoryMissingAndNotInRecoveryMode() {
        // Test scenario: Schema history is missing but not in recovery mode
        Offsets<SqlServerPartition, SqlServerOffsetContext> offsets = Offsets.of(mockPartition, mockOffsetContext);

        when(mockOffsetContext.isInitialSnapshotRunning()).thenReturn(false);
        when(mockHistorizedSchema.isHistorized()).thenReturn(true);
        when(mockHistorizedSchema.historyExists()).thenReturn(false);
        when(mockSnapshotter.shouldSnapshotOnSchemaError()).thenReturn(false);

        // Should throw DebeziumException about missing db history topic
        task.testValidateSchemaHistory(mockConnectorConfig, mockLogPositionValidator, offsets, mockHistorizedSchema, mockSnapshotter);
    }

    @Test
    public void testValidateSchemaHistoryInitializesStorageWhenHistoryMissingButInRecoveryMode() {
        // Test scenario: Schema history is missing but we're in recovery mode
        Offsets<SqlServerPartition, SqlServerOffsetContext> offsets = Offsets.of(mockPartition, mockOffsetContext);

        when(mockOffsetContext.isInitialSnapshotRunning()).thenReturn(false);
        when(mockHistorizedSchema.isHistorized()).thenReturn(true);
        when(mockHistorizedSchema.historyExists()).thenReturn(false);
        when(mockSnapshotter.shouldSnapshotOnSchemaError()).thenReturn(true);
        when(mockSnapshotter.name()).thenReturn("recovery");

        // Should not throw exception and should initialize storage
        task.testValidateSchemaHistory(mockConnectorConfig, mockLogPositionValidator, offsets, mockHistorizedSchema, mockSnapshotter);

        verify(mockHistorizedSchema).initializeStorage();
    }

    @Test
    public void testValidateSchemaHistoryWithLogPositionDisabled() {
        // Test scenario: Valid history exists and offset is valid
        Offsets<SqlServerPartition, SqlServerOffsetContext> offsets = Offsets.of(mockPartition, mockOffsetContext);

        when(mockOffsetContext.isInitialSnapshotRunning()).thenReturn(false);
        when(mockHistorizedSchema.isHistorized()).thenReturn(true);
        when(mockHistorizedSchema.historyExists()).thenReturn(true);
        when(mockConnectorConfig.isLogPositionCheckEnabled()).thenReturn(false);

        // Should not throw any exception and should not initialize storage
        task.testValidateSchemaHistory(mockConnectorConfig, mockLogPositionValidator, offsets, mockHistorizedSchema, mockSnapshotter);

        verify(mockHistorizedSchema, never()).initializeStorage();
    }

    @Test
    public void testValidateSchemaHistoryWithValidLogPosition() {
        // Test scenario: Valid history exists and offset is valid
        Offsets<SqlServerPartition, SqlServerOffsetContext> offsets = Offsets.of(mockPartition, mockOffsetContext);

        when(mockOffsetContext.isInitialSnapshotRunning()).thenReturn(false);
        when(mockHistorizedSchema.isHistorized()).thenReturn(true);
        when(mockHistorizedSchema.historyExists()).thenReturn(true);
        when(mockConnectorConfig.isLogPositionCheckEnabled()).thenReturn(true);
        when(mockLogPositionValidator.validate(mockPartition, mockOffsetContext, mockConnectorConfig)).thenReturn(true);
        // Should not throw any exception and should not initialize storage
        task.testValidateSchemaHistory(mockConnectorConfig, mockLogPositionValidator, offsets, mockHistorizedSchema, mockSnapshotter);

        verify(mockHistorizedSchema, never()).initializeStorage();
    }

    @Test
    public void testValidateSchemaHistoryWithUnavailableLogPosition() {
        // Test scenario: Valid history exists and offset is valid
        Offsets<SqlServerPartition, SqlServerOffsetContext> offsets = Offsets.of(mockPartition, mockOffsetContext);

        when(mockOffsetContext.isInitialSnapshotRunning()).thenReturn(false);
        when(mockHistorizedSchema.isHistorized()).thenReturn(true);
        when(mockHistorizedSchema.historyExists()).thenReturn(true);
        when(mockConnectorConfig.isLogPositionCheckEnabled()).thenReturn(true);
        when(mockLogPositionValidator.validate(mockPartition, mockOffsetContext, mockConnectorConfig)).thenReturn(false);
        when(mockSnapshotter.shouldStream()).thenReturn(true);

        // Should reset offset and take snashot
        when(mockSnapshotter.shouldSnapshotOnDataError()).thenReturn(false).thenReturn(true);

        assertThrows(DebeziumException.class,
                () -> task.testValidateSchemaHistory(mockConnectorConfig, mockLogPositionValidator, offsets, mockHistorizedSchema, mockSnapshotter));

        task.testValidateSchemaHistory(mockConnectorConfig, mockLogPositionValidator, offsets, mockHistorizedSchema, mockSnapshotter);

        assertNull(offsets.getTheOnlyOffset());
    }

    // Create a simple SqlServerTestTask task that exposes validateSchemaHistory
    public static class SqlServerTestTask extends SqlServerConnectorTask {
        // Expose the protected method for testing
        public void testValidateSchemaHistory(CommonConnectorConfig config,
                                              LogPositionValidator logPositionValidator,
                                              Offsets<SqlServerPartition, SqlServerOffsetContext> previousOffsets,
                                              DatabaseSchema schema,
                                              Snapshotter snapshotter) {
            validateSchemaHistory(config, logPositionValidator, previousOffsets, schema, snapshotter);
        }
    }
}
