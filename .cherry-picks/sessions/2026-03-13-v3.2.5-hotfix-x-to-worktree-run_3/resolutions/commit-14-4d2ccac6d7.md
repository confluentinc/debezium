# Resolution: #14 — Fail task if no tables to capture

| Field | Value |
|-------|-------|
| Source SHA | 4d2ccac6d7 |
| Result SHA | 959dd7cfd3 |
| Complexity | TRIVIAL |
| Conflict Files | `debezium-core/src/main/java/io/debezium/config/CommonConnectorConfig.java` |

## Strategy

The source commit adds a `failOnNoTables` boolean field, `FAIL_ON_NO_TABLES` config field, constructor init, and getter method. The target already has `guardrailCollectionsMax` and `guardrailCollectionsLimitAction` fields (from upstream DBZ-9427) at the same insertion points. Both are independent additions — resolution was to keep both sides (target's guardrail fields + source's failOnNoTables). For conflict #4, the source side also included `getEventProcessingFailureHandlingMode()` as context, which already exists in the target at line 1764, so it was not duplicated.

## Result Diff

4 conflict regions, all resolved by keeping both sides. The diff shows clean additions of `failOnNoTables` alongside existing guardrail fields.
