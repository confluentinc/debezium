# Resolution: #19 — Use re2j in EventRouter SMT instead of RegexRouter

| Field | Value |
|-------|-------|
| Source SHA | f2dcceb964 |
| Result SHA | ffc07f78e1 |
| Complexity | TRIVIAL |
| Conflict Files | `debezium-core/src/test/java/io/debezium/util/StringsTest.java` |

## Strategy

Single import conflict. Source removed `java.util.regex.Pattern` (replaced by `com.google.re2j.Pattern`). Target had added `java.util.function.Function` import nearby. Resolution: keep `Function` (used in the file), remove `java.util.regex.Pattern` (per Pattern 14 — prefer re2j). The `com.google.re2j.Pattern` import was already present cleanly at line 28.
