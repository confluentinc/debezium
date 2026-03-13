# Resolution: #25 — Fix sensitive data logging for debezium mysql

| Field | Value |
|-------|-------|
| Source SHA | efb6f872ad |
| Result SHA | 67c61686e4 |
| Complexity | TRIVIAL |
| Conflict Files | `debezium-connector-binlog/src/main/java/io/debezium/connector/binlog/BinlogStreamingChangeEventSource.java` |

## Strategy

Single conflict region. The commit's actual change was `LOGGER.debug` → `LOGGER.trace` for the DDL filter log line. Target had an upstream addition (`TRUNCATE_STATEMENT_PATTERN` check) in the condition. Resolution: keep target's condition with the truncate pattern check, apply the log level change from debug to trace.
