# Resolution: Commit #14 — Fail task if no tables to capture

| Field | Value |
|-------|-------|
| Source SHA | 4d2ccac6d7 |
| Result SHA | 5b899d3bf2 |
| Complexity | MODERATE |
| Conflict Files | `debezium-core/src/main/java/io/debezium/config/CommonConnectorConfig.java` |

## Strategy
The source commit adds a `failOnNoTables` boolean field, config definition (`FAIL_ON_NO_TABLES`), constructor initialization, and getter method. The target branch already has `guardrailCollectionsMax` and `guardrailCollectionsLimitAction` fields added at the same insertion points by an upstream commit (DBZ-9427). The conflicts were at 4 locations where both branches added new fields/config/getters at the same positions. Resolution: keep both — target's guardrail fields AND source's failOnNoTables field. Also avoided duplicating `getEventProcessingFailureHandlingMode()` which the source side included as context but already exists in the target.

## Result Diff (conflicted file only)
The diff shows clean additions of failOnNoTables alongside the existing guardrail fields at all 4 locations.
