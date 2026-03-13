# Resolution: Commit #25 - Fix sensitive data logging for debezium mysql

| Field | Value |
|-------|-------|
| Source SHA | efb6f872ad |
| Result SHA | 4cb935a785 |
| Complexity | TRIVIAL |
| Conflict Files | `debezium-connector-binlog/src/main/java/io/debezium/connector/binlog/BinlogStreamingChangeEventSource.java` |

## Strategy

Single conflict region. The source commit changes `LOGGER.debug` to `LOGGER.trace` for the DDL filter log line (sensitive data fix). The target branch (from upstream) added an additional `TRUNCATE_STATEMENT_PATTERN` check to the same `if` condition. Resolution: keep target's condition with the TRUNCATE_STATEMENT_PATTERN check, apply source's log level change from debug to trace.

## Result Diff (conflicted file only)

```diff
-        if (!TRUNCATE_STATEMENT_PATTERN.matcher(sql).matches() && schema.ddlFilter().test(sql)) {
-            LOGGER.debug("DDL '{}' was filtered out of processing", sql);
+        if (!TRUNCATE_STATEMENT_PATTERN.matcher(sql).matches() && schema.ddlFilter().test(sql)) {
+            LOGGER.trace("DDL '{}' was filtered out of processing", sql);
```
