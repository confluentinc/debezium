# Resolution: Commit #41 - Expose threadIds of new threads in Debezium CDC Source Connectors

| Field | Value |
|-------|-------|
| Source SHA | 9ab6be6a00 |
| Result SHA | 3c0ebf6e76 |
| Complexity | MODERATE |
| Conflict Files | `CommonConnectorConfig.java`, `MySqlConnectorTask.java`, `MariaDbConnectorTask.java`, `PostgresConnectorTask.java` |

## Strategy

7 conflict regions across 4 files. The commit adds `ThreadNameContext` support and `CONNECTOR_THREAD_NAME_PATTERN` config field. All conflicts follow the same pattern: target has guardrail fields or heartbeat API differences, source adds thread-related code.

### CommonConnectorConfig.java (3 conflicts)

1. **Field definitions (line ~1391):** Target has `GUARDRAIL_COLLECTIONS_MAX` and `GUARDRAIL_COLLECTIONS_LIMIT_ACTION` fields. Source adds `CONNECTOR_THREAD_NAME_PATTERN`. Resolution: kept both — placed `CONNECTOR_THREAD_NAME_PATTERN` before the guardrail fields.

2. **CONFIG_DEFINITION list (line ~1466):** Target has `GUARDRAIL_COLLECTIONS_MAX, GUARDRAIL_COLLECTIONS_LIMIT_ACTION, FAIL_ON_NO_TABLES)`. Source has `FAIL_ON_NO_TABLES, CONNECTOR_THREAD_NAME_PATTERN)`. Resolution: kept all four fields in the list.

3. **Methods (line ~2088):** Target has `@Deprecated createHeartbeat(...)`. Source adds `getConnectorTaskId()` method. Resolution: kept both — placed `getConnectorTaskId()` before the deprecated annotation.

### MySqlConnectorTask.java (1 conflict, line ~222)

Target has `MySqlFieldReaderResolver.resolve(connectorConfig)),` with `queue)` parameter in heartbeat. Source adds `ThreadNameContext.from(connectorConfig)` as extra param to `MySqlConnection` constructor but removes `queue)`. The commit's actual diff only adds `ThreadNameContext` — the `queue` difference is pre-existing between branches. Resolution: added `ThreadNameContext.from(connectorConfig)` AND kept target's `queue)` parameter.

### MariaDbConnectorTask.java (1 conflict, line ~228)

Same pattern as MySQL. Target uses `new MariaDbConnectionConfiguration(config)` with `queue)`. Source uses `heartbeatConfig` (pre-existing difference) and adds `ThreadNameContext`. Resolution: kept target's `config` variable and `queue)`, added `ThreadNameContext.from(connectorConfig)`.

### PostgresConnectorTask.java (1 conflict, line ~223)

Target uses `HeartbeatFactory<>().getScheduledHeartbeat(connectorConfig, ...)`. Source uses `connectorConfig.createHeartbeat(topicNamingStrategy, schemaNameAdjuster, ...)` — a different heartbeat API (pre-existing difference). The commit only adds `threadNameContext` to the `PostgresConnection` constructor. Resolution: kept target's `HeartbeatFactory` API, added `threadNameContext` to the connection constructor.

## Result Diff (conflicted files only)

### CommonConnectorConfig.java

```diff
+    public static final Field CONNECTOR_THREAD_NAME_PATTERN = Field.create("connector.thread.name.pattern")
+            .withDisplayName("Connector Thread Name Pattern")
+            .withType(Type.STRING)
+            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 32))
+            .withWidth(Width.MEDIUM)
+            .withImportance(Importance.LOW)
+            .optional()
+            .withDefault("${debezium}-${connector.class.simple}-${topic.prefix}-${functionality}")
+            .withDescription(
+                    "The pattern used to name the threads created during connector lifetime. "
+                            + "The default value is '${debezium}-${connector.class.simple}-${topic.prefix}-${functionality}'. "
+                            + "Available variables are: ${debezium}, ${connector.class.simple}, ${topic.prefix}, ${functionality} "
+                            + "${connector.name} and ${task.id} to include connector name and task id in thread names. "
+                            + "Custom patterns can be specified while maintaining the default structure.");
+
     public static final Field GUARDRAIL_COLLECTIONS_MAX = Field.create("guardrail.collections.max")
```

```diff
                     GUARDRAIL_COLLECTIONS_LIMIT_ACTION,
-                    FAIL_ON_NO_TABLES)
+                    FAIL_ON_NO_TABLES,
+                    CONNECTOR_THREAD_NAME_PATTERN)
```

```diff
+    public String getConnectorTaskId() {
+        if (taskId == null || taskId.isEmpty()) {
+            return "0";
+        }
+        return taskId;
+    }
+
     /**
      * @deprecated Use {@link io.debezium.heartbeat.HeartbeatFactory} instead.
      */
     @Deprecated
```

### MySqlConnectorTask.java

```diff
                         () -> new MySqlConnection(
                                 new MySqlConnectionConfiguration(heartbeatConfig),
-                                MySqlFieldReaderResolver.resolve(connectorConfig)),
+                                MySqlFieldReaderResolver.resolve(connectorConfig), ThreadNameContext.from(connectorConfig)),
                         new BinlogHeartbeatErrorHandler(),
                         queue),
```

### MariaDbConnectorTask.java

```diff
                         () -> new MariaDbConnection(
                                 new MariaDbConnectionConfiguration(config),
-                                getFieldReader(connectorConfig)),
+                                getFieldReader(connectorConfig), ThreadNameContext.from(connectorConfig)),
                         new BinlogHeartbeatErrorHandler(),
                         queue),
```

### PostgresConnectorTask.java

```diff
                     new HeartbeatFactory<>().getScheduledHeartbeat(
                             connectorConfig,
-                            () -> new PostgresConnection(connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL),
+                            () -> new PostgresConnection(connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL, threadNameContext),
```
