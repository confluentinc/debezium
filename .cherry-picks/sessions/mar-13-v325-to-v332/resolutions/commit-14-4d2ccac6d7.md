# Resolution: Commit #14 - Fail task if no tables to capture

| Field | Value |
|-------|-------|
| Source SHA | 4d2ccac6d7 |
| Result SHA | a42403cf74 |
| Complexity | TRIVIAL |
| Conflict Files | `debezium-core/src/main/java/io/debezium/config/CommonConnectorConfig.java` |

## Strategy

The source commit adds a new `failOnNoTables` field, config constant `FAIL_ON_NO_TABLES`, constructor init, and getter method. The target branch has guardrail fields (`guardrailCollectionsMax`, `guardrailCollectionsLimitAction`) that the source branch doesn't have — these were added at the same insertion points, causing 4 conflict regions.

Resolution: Keep both — target's guardrail fields AND source's `failOnNoTables`. All 4 conflicts were the same "keep both sides" pattern. For conflict #4 (getter methods), the source side also included `getEventProcessingFailureHandlingMode()` as context, which already exists in the target — excluded to avoid duplication.

## Result Diff (conflicted file only)

```diff
+    protected final boolean failOnNoTables;

+    public static final Field FAIL_ON_NO_TABLES = Field.createInternal("fail.on.no.tables")
+            .withDisplayName("Fail if no tables are found")
+            .withType(Type.BOOLEAN)
+            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 999))
+            .withWidth(Width.SHORT)
+            .withImportance(Importance.LOW)
+            .withDescription("Fail if no tables are found that match the configured filters.")
+            .withDefault(true);

-                    GUARDRAIL_COLLECTIONS_LIMIT_ACTION)
+                    GUARDRAIL_COLLECTIONS_LIMIT_ACTION,
+                    FAIL_ON_NO_TABLES)

+        this.failOnNoTables = config.getBoolean(FAIL_ON_NO_TABLES);

+    public boolean failOnNoTables() {
+        return failOnNoTables;
+    }
```
