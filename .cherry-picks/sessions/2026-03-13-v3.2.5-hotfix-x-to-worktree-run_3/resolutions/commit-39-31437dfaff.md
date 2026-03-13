# Resolution: #39 — Refactor avoid certain statements from history topic

| Field | Value |
|-------|-------|
| Source SHA | 31437dfaff |
| Result SHA | c1cede24bc |
| Complexity | MODERATE |
| Conflict Files | `BinlogStreamingChangeEventSource.java`, `SchemaHistory.java` |

## Strategy

3 conflict regions across 2 files.

1. **BinlogStreamingChangeEventSource.java (constants):** Commit removes `SET_STATEMENT_REGEX` and `DDL_SKIP_PATTERN`, adds `TRUNCATE_STATEMENT_PATTERN`. Target already had all three from prior commits. Resolution: remove `SET_STATEMENT_REGEX` and `DDL_SKIP_PATTERN` per refactor intent, keep `TRUNCATE_STATEMENT_PATTERN`.

2. **BinlogStreamingChangeEventSource.java (filter logic):** Source uses `.matches(sql)` (re2j Pattern method), target uses `.matcher(sql).matches()` (java.util.regex). Both Patterns are imported. Since `TRUNCATE_STATEMENT_PATTERN` uses `Pattern.DOTALL` (java.util.regex flag), kept target's `.matcher(sql).matches()` to avoid ambiguity.

3. **SchemaHistory.java (ddlFilter):** Source added VIEW/FUNCTION/PROCEDURE/TRIGGER filter. Target already had additional GRANT/REVOKE, ANALYZE/OPTIMIZE, USER/ROLE patterns from upstream. Resolution: keep target's superset patterns plus the VIEW/FUNCTION/PROCEDURE/TRIGGER line (which was already present right after the conflict block).
