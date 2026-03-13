# Resolution: #38 — Prevent trigger,function,view and procedure from history topic

| Field | Value |
|-------|-------|
| Source SHA | bb0c076cfa |
| Result SHA | d5a7550acf |
| Complexity | TRIVIAL |
| Conflict Files | `BinlogStreamingChangeEventSource.java`, `BinlogRegressionIT.java` |

## Strategy

3 conflict regions. (1) Source adds `DDL_SKIP_PATTERN` constant where target has `TRUNCATE_STATEMENT_PATTERN` — kept both. (2,3) Test file had identical code on both sides (`numCreateDefiner = 0`) with different comments — kept target's upstream reference (DBZ-9186).
