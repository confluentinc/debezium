/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Integration test for multi-task snapshot with table distribution.
 * Tests that the connector properly distributes tables across multiple tasks
 * when tasks.max > 1.
 *
 * @author Debezium Authors
 */
public class MultiTaskSnapshotIT {

    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS inventory CASCADE;" +
            "CREATE SCHEMA inventory; " +
            "CREATE TABLE inventory.products (pk SERIAL, name varchar(255), PRIMARY KEY(pk));" +
            "CREATE TABLE inventory.orders (pk SERIAL, product_id integer, PRIMARY KEY(pk));" +
            "CREATE TABLE inventory.customers (pk SERIAL, name varchar(255), PRIMARY KEY(pk));" +
            "CREATE TABLE inventory.items (pk SERIAL, description varchar(255), PRIMARY KEY(pk));" +
            "CREATE TABLE inventory.suppliers (pk SERIAL, name varchar(255), PRIMARY KEY(pk));" +
            "CREATE TABLE inventory.shipments (pk SERIAL, order_id integer, PRIMARY KEY(pk));";

    private static final String INSERT_STMT = "INSERT INTO inventory.products (name) VALUES ('Widget');" +
            "INSERT INTO inventory.orders (product_id) VALUES (1);" +
            "INSERT INTO inventory.customers (name) VALUES ('John Doe');" +
            "INSERT INTO inventory.items (description) VALUES ('Item 1');" +
            "INSERT INTO inventory.suppliers (name) VALUES ('Acme Corp');" +
            "INSERT INTO inventory.shipments (order_id) VALUES (1);";

    private PostgresConnector connector;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        connector = new PostgresConnector();
    }

    @After
    public void after() {
        if (connector != null) {
            connector.stop();
        }
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Test
    @FixFor("DBZ-MULTI-TASK")
    public void shouldDistributeTablesAcrossMultipleTasks() throws Exception {
        // Setup: Create 6 tables in the database
        TestHelper.execute(CREATE_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);

        // Create connector configuration with table include list
        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, "inventory\\..*")
                .build();

        // Start the connector
        connector.start(config.asMap());

        // Test with maxTasks = 3 (should create 3 tasks with 2 tables each)
        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);

        assertThat(taskConfigs).hasSize(3);

        // Verify each task has a unique task ID
        Set<String> taskIds = taskConfigs.stream()
                .map(props -> props.get(CommonConnectorConfig.TASK_ID))
                .collect(Collectors.toSet());
        assertThat(taskIds).containsExactlyInAnyOrder("0", "1", "2");

        // Collect all assigned tables across all tasks
        Set<String> allAssignedTables = new HashSet<>();
        for (Map<String, String> taskConfig : taskConfigs) {
            String tableIncludeList = taskConfig.get(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name());
            assertThat(tableIncludeList).isNotNull();
            assertThat(tableIncludeList).isNotEmpty();

            // Split the table list and add to set
            String[] tables = tableIncludeList.split(",");
            allAssignedTables.addAll(Arrays.asList(tables));

            // Verify table.exclude.list is removed
            assertThat(taskConfig.get(RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST.name())).isNull();
        }

        // Verify all 6 tables are covered (no duplicates, no missing tables)
        assertThat(allAssignedTables).hasSize(6);
        assertThat(allAssignedTables).containsExactlyInAnyOrder(
                "inventory.products",
                "inventory.orders",
                "inventory.customers",
                "inventory.items",
                "inventory.suppliers",
                "inventory.shipments");
    }

    @Test
    @FixFor("DBZ-MULTI-TASK")
    public void shouldCapTasksToNumberOfTables() throws Exception {
        // Setup: Create 6 tables
        TestHelper.execute(CREATE_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);

        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, "inventory\\..*")
                .build();

        connector.start(config.asMap());

        // Request 10 tasks but should only get 6 (one per table)
        List<Map<String, String>> taskConfigs = connector.taskConfigs(10);

        assertThat(taskConfigs).hasSize(6);

        // Each task should have exactly 1 table
        for (Map<String, String> taskConfig : taskConfigs) {
            String tableIncludeList = taskConfig.get(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name());
            assertThat(tableIncludeList).doesNotContain(","); // No comma means single table
        }
    }

    @Test
    @FixFor("DBZ-MULTI-TASK")
    public void shouldUseSingleTaskWhenMaxTasksIsOne() throws Exception {
        TestHelper.execute(CREATE_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);

        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, "inventory\\..*")
                .build();

        connector.start(config.asMap());

        // With maxTasks = 1, should return original config (single task)
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

        assertThat(taskConfigs).hasSize(1);

        // The original table.include.list should be preserved (regex pattern)
        Map<String, String> taskConfig = taskConfigs.get(0);
        String tableIncludeList = taskConfig.get(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name());
        assertThat(tableIncludeList).isEqualTo("inventory\\..*");
    }

    @Test
    @FixFor("DBZ-MULTI-TASK")
    public void shouldHandleExplicitTableList() throws Exception {
        TestHelper.execute(CREATE_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);

        // Use explicit table names instead of regex
        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
                        "inventory.products,inventory.orders,inventory.customers")
                .build();

        connector.start(config.asMap());

        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);

        // Should have 3 tasks with 1 table each
        assertThat(taskConfigs).hasSize(3);

        Set<String> allTables = new HashSet<>();
        for (Map<String, String> taskConfig : taskConfigs) {
            String tableIncludeList = taskConfig.get(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name());
            allTables.add(tableIncludeList);
        }

        assertThat(allTables).containsExactlyInAnyOrder(
                "inventory.products",
                "inventory.orders",
                "inventory.customers");
    }

    @Test
    @FixFor("DBZ-MULTI-TASK")
    public void shouldRoundRobinDistributeUnevenTableCount() throws Exception {
        TestHelper.execute(CREATE_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);

        // Use 5 tables with 3 tasks -> 2, 2, 1 distribution
        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
                        "inventory.products,inventory.orders,inventory.customers,inventory.items,inventory.suppliers")
                .build();

        connector.start(config.asMap());

        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);

        assertThat(taskConfigs).hasSize(3);

        // Count tables per task
        int[] tablesPerTask = new int[3];
        for (int i = 0; i < taskConfigs.size(); i++) {
            String tableIncludeList = taskConfigs.get(i).get(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name());
            tablesPerTask[i] = tableIncludeList.split(",").length;
        }

        // Round-robin distribution: task 0 and 1 get 2 tables, task 2 gets 1 table
        assertThat(tablesPerTask[0]).isEqualTo(2);
        assertThat(tablesPerTask[1]).isEqualTo(2);
        assertThat(tablesPerTask[2]).isEqualTo(1);
    }

    @Test
    @FixFor("DBZ-MULTI-TASK")
    public void shouldHandleSingleTable() throws Exception {
        // Create only one table
        TestHelper.execute(
                "DROP SCHEMA IF EXISTS single CASCADE;" +
                        "CREATE SCHEMA single; " +
                        "CREATE TABLE single.only_table (pk SERIAL, PRIMARY KEY(pk));" +
                        "INSERT INTO single.only_table (pk) VALUES (1);");

        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, "single\\..*")
                .build();

        connector.start(config.asMap());

        // Even with maxTasks = 3, should only get 1 task (can't have more tasks than tables)
        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);

        assertThat(taskConfigs).hasSize(1);
    }

    @Test
    @FixFor("DBZ-MULTI-TASK")
    public void shouldPreserveOtherConfigProperties() throws Exception {
        TestHelper.execute(CREATE_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);

        String customProperty = "custom.property";
        String customValue = "custom-value";

        Map<String, String> configMap = new HashMap<>(TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, "inventory\\..*")
                .build()
                .asMap());
        configMap.put(customProperty, customValue);

        connector.start(configMap);

        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);

        // Verify that custom properties are preserved in all task configs
        for (Map<String, String> taskConfig : taskConfigs) {
            assertThat(taskConfig.get(customProperty)).isEqualTo(customValue);
            // Verify core config is also preserved
            assertThat(taskConfig.get(PostgresConnectorConfig.HOSTNAME.name())).isNotNull();
        }
    }

    @Test
    @FixFor("DBZ-MULTI-TASK")
    public void shouldHandleSchemaIncludeListWithMultipleTasks() throws Exception {
        // Create tables in multiple schemas
        TestHelper.execute(
                "DROP SCHEMA IF EXISTS schema1 CASCADE;" +
                        "DROP SCHEMA IF EXISTS schema2 CASCADE;" +
                        "CREATE SCHEMA schema1; " +
                        "CREATE SCHEMA schema2; " +
                        "CREATE TABLE schema1.table_a (pk SERIAL, PRIMARY KEY(pk));" +
                        "CREATE TABLE schema1.table_b (pk SERIAL, PRIMARY KEY(pk));" +
                        "CREATE TABLE schema2.table_c (pk SERIAL, PRIMARY KEY(pk));" +
                        "CREATE TABLE schema2.table_d (pk SERIAL, PRIMARY KEY(pk));" +
                        "INSERT INTO schema1.table_a (pk) VALUES (1);" +
                        "INSERT INTO schema1.table_b (pk) VALUES (1);" +
                        "INSERT INTO schema2.table_c (pk) VALUES (1);" +
                        "INSERT INTO schema2.table_d (pk) VALUES (1);");

        Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST, "schema1,schema2")
                .build();

        connector.start(config.asMap());

        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);

        assertThat(taskConfigs).hasSize(2);

        // Collect all tables
        Set<String> allTables = new HashSet<>();
        for (Map<String, String> taskConfig : taskConfigs) {
            String tableIncludeList = taskConfig.get(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST.name());
            allTables.addAll(Arrays.asList(tableIncludeList.split(",")));
        }

        assertThat(allTables).hasSize(4);
    }

    @Test
    @FixFor("DBZ-MULTI-TASK")
    public void shouldReturnEmptyListWhenNotStarted() {
        // Don't start the connector
        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);

        assertThat(taskConfigs).isEmpty();
    }
}
