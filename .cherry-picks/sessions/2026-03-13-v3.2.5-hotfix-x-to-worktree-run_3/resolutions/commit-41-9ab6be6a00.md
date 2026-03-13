# Resolution: #41 — Expose threadIds of the new threads created

| Field | Value |
|-------|-------|
| Source SHA | 9ab6be6a00 |
| Result SHA | a126f39d10 |
| Complexity | MODERATE |
| Conflict Files | `MariaDbConnectorTask.java`, `MySqlConnectorTask.java`, `PostgresConnectorTask.java`, `CommonConnectorConfig.java` |

## Strategy

7 conflict regions across 4 files.

1. **Connector task files (MariaDb, MySQL, Postgres):** Source used `createHeartbeat()` with `ThreadNameContext` param. Target uses `HeartbeatFactory.getScheduledHeartbeat()` with a `queue` param (upstream API). Kept target's heartbeat API — the ThreadNameContext was applied elsewhere (in BinaryLogClient setup) by the clean parts of the commit.

2. **CommonConnectorConfig (field definition):** Source adds `CONNECTOR_THREAD_NAME_PATTERN` where target has guardrail fields. Kept both.

3. **CommonConnectorConfig (CONFIG_DEFINITION):** Added `CONNECTOR_THREAD_NAME_PATTERN` to the existing config list alongside guardrail fields and `FAIL_ON_NO_TABLES`.

4. **CommonConnectorConfig (getConnectorTaskId):** Source adds new `getConnectorTaskId()` method where target has `@Deprecated createHeartbeat()`. Kept both — added `getConnectorTaskId()` before the deprecated method.
