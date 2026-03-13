# Resolution: Commit #39 - Refactor avoid certain statements from history topic

| Field | Value |
|-------|-------|
| Source SHA | 31437dfaff |
| Result SHA | c7914ac0a3 |
| Complexity | MODERATE |
| Conflict Files | `BinlogStreamingChangeEventSource.java`, `SchemaHistory.java` |

## Strategy

3 conflict regions across 2 files:

1. **BinlogStreamingChangeEventSource.java (constants):** Source removes `SET_STATEMENT_REGEX` and `DDL_SKIP_PATTERN`, keeps only `TRUNCATE_STATEMENT_PATTERN`. Target had all three. Accepted source's removal — the DDL skip filtering is now moved to SchemaHistory's ddlFilter. Used `com.google.re2j.Pattern` import (replacing `java.util.regex.Pattern`) to match the source branch's re2j usage introduced in commit #19.

2. **BinlogStreamingChangeEventSource.java (filter condition):** Source uses `.matches(sql)` (re2j method), target uses `.matcher(sql).matches()` (java.util.regex). Kept source's `.matches(sql)` since the file now uses `com.google.re2j.Pattern`.

3. **SchemaHistory.java (DDL filter patterns):** The source commit adds 3 new DDL filter patterns to the `DDL_FILTER` field:
   - `(SET STATEMENT .*)?TRUNCATE TABLE .*`
   - `(SET STATEMENT .*)?REPLACE INTO .*`
   - `^(?:SET STATEMENT\\s+.*?FOR\\s+)?(CREATE|ALTER|DROP)\\s+...?(VIEW|FUNCTION|PROCEDURE|TRIGGER)\\s+.*`

   The target (v3.3.2 upstream) already had the TRUNCATE and REPLACE INTO patterns (from DBZ-9186), **plus** additional upstream patterns that the source branch didn't have:
   - `^(?:SET STATEMENT\\s+.*?FOR\\s+)?(?:GRANT|REVOKE)\\s+.*` — filters GRANT/REVOKE statements
   - `^(?:SET STATEMENT\\s+.*?FOR\\s+)?(?:ANALYZE|OPTIMIZE|REPAIR)\\s+(?:NO_WRITE_TO_BINLOG\\s+|LOCAL\\s+)?TABLE\\s+.*` — filters ANALYZE/OPTIMIZE/REPAIR TABLE
   - `^(?:SET STATEMENT\\s+.*?FOR\\s+)?(CREATE|ALTER|DROP)\\s+(?:OR\\s+REPLACE\\s+)?(?:IF\\s+(?:NOT\\s+)?EXISTS\\s+)?(USER|ROLE)\\b.*` — filters CREATE/ALTER/DROP USER/ROLE

   **Resolution:** Kept all target's upstream patterns AND the source's VIEW/FUNCTION/PROCEDURE/TRIGGER pattern (which was auto-merged outside the conflict region). The final DDL_FILTER in SchemaHistory.java contains all patterns from both branches:
   - Original upstream patterns (Dummy event, SAVEPOINT, rds_monitor, etc.)
   - TRUNCATE TABLE, REPLACE INTO (both branches had these)
   - GRANT/REVOKE (target upstream only)
   - ANALYZE/OPTIMIZE/REPAIR TABLE (target upstream only)
   - CREATE/ALTER/DROP USER/ROLE (target upstream only)
   - CREATE/ALTER/DROP VIEW/FUNCTION/PROCEDURE/TRIGGER (source commit's addition, auto-merged)

   The net result for SchemaHistory.java: no diff from target baseline because the source's new VIEW/FUNCTION/PROCEDURE/TRIGGER pattern was auto-merged (outside conflict region), and the conflict was resolved by keeping the target's additional upstream patterns.

## Result Diff (conflicted files only)

```diff
# BinlogStreamingChangeEventSource.java
-import java.util.regex.Pattern;
+import com.google.re2j.Pattern;

-    private static final String SET_STATEMENT_REGEX = "SET STATEMENT .* FOR";
-    private static final Pattern DDL_SKIP_PATTERN = Pattern.compile(
-            ".*\\b(CREATE|ALTER|DROP)\\b.*?\\b(VIEW|FUNCTION|PROCEDURE|TRIGGER)\\b.*",
-            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
# (kept only TRUNCATE_STATEMENT_PATTERN with re2j .matches(sql))

-        if (!TRUNCATE_STATEMENT_PATTERN.matcher(sql).matches() && schema.ddlFilter().test(sql)) {
+        if (!TRUNCATE_STATEMENT_PATTERN.matches(sql) && schema.ddlFilter().test(sql)) {

# SchemaHistory.java - conflict resolved by keeping target's upstream patterns:
# GRANT/REVOKE, ANALYZE/OPTIMIZE/REPAIR TABLE, CREATE/ALTER/DROP USER/ROLE
# Source's VIEW/FUNCTION/PROCEDURE/TRIGGER pattern was auto-merged (no diff in conflict region)
```
