# Cherry-Pick Progress: v3.2.5-hotfix-x → v3.3.2.Final

- **Source:** `v3.2.5-hotfix-x`
- **Target:** `worktree-run_manual` (based on `v3.3.2.Final`)
- **Working branch:** `mar-13-2026-debezium-cherry-picks-v332`
- **Start commit:** `52d4dbd3d0` (`[maven-release-plugin] prepare release v3.2.5.Final`)
- **Total commits:** 94
- **Upstream skips:** 9 (already in v3.3.2.Final)
- **To pick:** 85

## Summary
- CLEAN: 68
- RESOLVED: 12
- SKIPPED: 14 (9 upstream + 5 empty)
- PENDING: 0

## Progress

| # | SHA | Subject | Status | Complexity | Conflicts | Resolution Summary | Result Commit | Upstream Match |
|---|-----|---------|--------|------------|-----------|-------------------|---------------|----------------|
| 1 | c5e6da03e6 | Remove unwanted modules | CLEAN | - | - | - | e920ecbade | - |
| 2 | 8608898351 | Add Semaphore CI | CLEAN | - | - | - | 1a73deb2f4 | - |
| 3 | dfbf907156 | Downgrade maven version | CLEAN | - | - | - | 7e574fe74d | - |
| 4 | eced94ce1e | Use pgoutput decoder in build profile | CLEAN | - | - | - | 28a15a9743 | - |
| 5 | e4166076b5 | CC-32941 -- Fix MySQL CI | CLEAN | - | - | - | b570de905e | - |
| 6 | b23e2dea11 | Ignore flaky test in mysql (#132) | CLEAN | - | - | - | 2c13f74fe5 | - |
| 7 | 6a2f831990 | Remove logging for configs | CLEAN | - | - | - | ee249c516c | - |
| 8 | e100683437 | Mask CCloud Kafka secrets in logs | CLEAN | - | - | - | 163278f6e4 | - |
| 9 | cbb15be133 | Update dependency (#119) | CLEAN | - | - | - | 97d475267f | - |
| 10 | cf2fb50183 | Validate server version for pgoutput plugin (#120) | CLEAN | - | - | - | 5bc985491c | - |
| 11 | 1c2f288b7c | Handle exception thrown in validation (#121) | CLEAN | - | - | - | 9f1bbd773b | - |
| 12 | 2118e9d6f6 | Sanitize AuthenticationException error messages (#123) | CLEAN | - | - | - | 12eb6f20da | - |
| 13 | 11d74ab00f | Convert Debezium Snapshot & Schema Metrics to integer (#124) | CLEAN | - | - | - | 6fde901138 | - |
| 14 | 4d2ccac6d7 | Fail task if no tables to capture (#125) | RESOLVED | TRIVIAL | CommonConnectorConfig.java | Keep both: target guardrail fields + source failOnNoTables | a42403cf74 | - |
| 15 | 9e14700759 | Validate connection without checking errors on topic.prefix (#127) | CLEAN | - | - | - | c7834f844b | - |
| 16 | cce418286a | V2 wrappers and shading (#128) | CLEAN | - | - | - | c9911e83c6 | - |
| 17 | 285eaac5f1 | Move commitOffset functionality from OffsetCommiter thread to task-thread (#129) | CLEAN | - | - | - | e2d1f08e77 | - |
| 18 | 4fc9cfe875 | Return delete.tombstone.handling.mode from ExtractNewRecordState config method | SKIPPED | - | - | Already in target via upstream | - | 54a7084978 via Message match (DBZ-8776) |
| 19 | f2dcceb964 | Use re2j in EventRouter SMT instead of RegexRouter (#117) | RESOLVED | TRIVIAL | StringsTest.java | Import conflict: kept target Function import, removed java.util.regex.Pattern (replaced by re2j) | cb591148ae | - |
| 20 | 34b097a95f | Exclude non-required dependencies (#135) | CLEAN | - | - | - | 89f904cf46 | - |
| 21 | 4e1980df8d | Add logs when there is config value error | CLEAN | - | - | - | ab4d035189 | - |
| 22 | c952a4e1d1 | Update condition check on server id | CLEAN | - | - | - | 14b3fb06d7 | - |
| 23 | af55e7ac97 | Cherry-pick #74: Define limits on length of strings to be matched with regexes | CLEAN | - | - | - | 4c6fec3a70 | - |
| 24 | db613ced1c | CC-33246: Fix sensitive data logging for debezium postgres (#143) | CLEAN | - | - | - | 494d1c1561 | - |
| 25 | efb6f872ad | CC-32477 - Fix sensitive data logging for debezium mysql (#147) | RESOLVED | TRIVIAL | BinlogStreamingChangeEventSource.java | Kept target TRUNCATE_STATEMENT_PATTERN condition, applied source debug→trace log level change | 4cb935a785 | - |
| 26 | 718cf524da | Fix lint and formatting (#148) | CLEAN | - | - | - | 7a3b2cc3be | - |
| 27 | d18ec28d76 | CC-33853 -- Temporarily re-enable additional-condition for backward compat | CLEAN | - | - | - | 7f5ddcaab4 | - |
| 28 | 5d22428197 | CC-33248: Fix sensitive data logging for debezium sqlserver (#144) | CLEAN | - | - | - | f2dbf72be8 | - |
| 29 | 258c2e12de | Migrate Ubuntu to fix Semaphore build (#154) | CLEAN | - | - | - | cabfa23b4c | - |
| 30 | 7e65e1f4a2 | Fix lint and import suggestions (#158) | SKIPPED | - | - | Empty cherry-pick (already in target) | - | - |
| 31 | 7ae9d5be33 | CC-34037: Cherrypick IAM AssumeRole support in MySQL CDC changes to V3 (#159) | CLEAN | - | - | - | 528f339fe6 | - |
| 32 | a14b23751a | Fix compile failure for common mvn build (#162) | CLEAN | - | - | - | 7e698011f8 | - |
| 33 | 1d610c5bda | Add mariadb grammar statements back to mysql grammar (#164) | CLEAN | - | - | - | 63abf53977 | - |
| 34 | bf6fa53a1d | Remove support for ts_us and ts_ns (#165) | CLEAN | - | - | - | 6bf2a12226 | - |
| 35 | 11db8ed7ac | Adding mariadb to service.yml and semaphore.yml (#168) | CLEAN | - | - | - | 525aa7f770 | - |
| 36 | 8c8d796c8c | [CC-34752] Perform regex match only in some cases since it's costly CPU wise | CLEAN | - | - | - | ce536c1330 | - |
| 37 | 91a3e94903 | CC-34563 Remove column name from warn log (#173) | CLEAN | - | - | - | 93577df8c4 | - |
| 38 | bb0c076cfa | CC-34779 Prevent tigger,function,view and procedure from history topic (#175) | RESOLVED | TRIVIAL | BinlogStreamingChangeEventSource.java, BinlogRegressionIT.java | Kept both TRUNCATE_STATEMENT_PATTERN and DDL_SKIP_PATTERN; kept target DBZ-9186 comment | f3c01aab58 | - |
| 39 | 31437dfaff | CC-34771 -- Refactor avoid certain statements from history topic (#179) | RESOLVED | MODERATE | BinlogStreamingChangeEventSource.java, SchemaHistory.java | Removed SET_STATEMENT_REGEX and DDL_SKIP_PATTERN; used re2j Pattern with .matches(sql); kept upstream DDL filter patterns in SchemaHistory | c7914ac0a3 | - |
| 40 | 210b5f979d | DBZ-9158: cherry picking publication change (#183) | SKIPPED | - | - | Already in target via upstream | - | 8a6dbc107d via DBZ ticket (DBZ-9158) |
| 41 | 9ab6be6a00 | Expose threadIds of the new threads created in Debezium CDC Source Connectors | RESOLVED | MODERATE | CommonConnectorConfig.java, MySqlConnectorTask.java, MariaDbConnectorTask.java, PostgresConnectorTask.java | Kept both guardrail + thread fields; added ThreadNameContext to connection constructors while preserving target's queue param and HeartbeatFactory API | 3c0ebf6e76 | - |
| 42 | f548889260 | CC-34585 Fix usage of cached prepared statement in emitWindowOpen (#186) | CLEAN | - | - | - | bc6e473ac0 | - |
| 43 | d4a8de1884 | Add ThreadNameContext to PostgresEventStreamingSource new Thread method (#191) | RESOLVED | TRIVIAL | PostgresStreamingChangeEventSource.java | Applied ThreadNameContext to target's existing lsnFlushExecutor at constructor line 116, kept target's refactored approach | f69ca4268f | - |
| 44 | 66ca661c06 | Validation ConfigDef Injection (#192) | CLEAN | - | - | - | 4c2de7f688 | - |
| 45 | ef98e0450b | Validation ConfigDef Injection (#192) | SKIPPED | - | - | Empty cherry-pick (same as #44) | - | - |
| 46 | 2496087887 | Publication Timeouts (#194) | RESOLVED | TRIVIAL | PostgresReplicationConnection.java | Target already has executeWithTimeout; kept all 3 conflict regions as HEAD | 72dc946bd5 | - |
| 47 | be66546aa1 | CC-35215: Fixes Found a not connector specific implementation warn (#193) | CLEAN | - | - | - | 9e7f588473 | - |
| 48 | 6fa4258054 | CC-34772: Connector failing to restart due to special table in Incremental snapshot | SKIPPED | - | - | Empty cherry-pick (already in target) | - | - |
| 49 | d6aee1df97 | Fix thread naming error during fail-fast validation when connector name is null | CLEAN | - | - | - | 5e4867546a | - |
| 50 | 8324e0594d | CC-36321: DBZ-9395: Selectively call ALTER PUBLICATION for filtered publication | SKIPPED | - | - | Already in target via upstream | - | a8e1557b32 via DBZ ticket (DBZ-9395) |
| 51 | 174c53436f | CC-35868: Memory should relinquished after the task is stopped (#190) | CLEAN | - | - | - | aaba1a7973 | - |
| 52 | 388e28d1a6 | Adds a sleep in the SQL Server IT to ensure the server is ready to serve requests | CLEAN | - | - | - | 7356586051 | - |
| 53 | 4a7f0b1217 | CC-35836: Adds IT for log position validation method in SQL Server (#202) | CLEAN | - | - | - | f854de40a2 | - |
| 54 | 2a10819175 | CC-35228: Add Credential Provider support for Postgres (#198) | CLEAN | - | - | - | 4bfc0add33 | - |
| 55 | b6bcc4913c | Bump Credential Provider Version (#204) | CLEAN | - | - | - | 234bf1bd0b | - |
| 56 | e7ea34166d | CC-36147 - Cherry-pick DBZ-9427: Introduce guardrail to limit number of tables | SKIPPED | - | - | Already in target via upstream | - | a920481423 via DBZ ticket (DBZ-9427) |
| 57 | dd947575bd | CC-35558: Remove the LsnSeen cache (#205) | CLEAN | - | - | - | 17aa69382d | - |
| 58 | 1b49e6c4e0 | Timezone Validation Error Fix (#207) | CLEAN | - | - | - | dcf10c451b | - |
| 59 | 5cee5df339 | CC-36490: Add read access verification for schema history topic during startup | CLEAN | - | - | - | 1e35b90a81 | - |
| 60 | 9ddaef7f4c | INC-6146: Fix log position validation bug (#206) | CLEAN | - | - | - | c86858a817 | - |
| 61 | 412bd87810 | Verify schema history read access only for historized schema based connectors | CLEAN | - | - | - | 5dda0716c3 | - |
| 62 | 1fc3a307e2 | CC-37431: Gracefully cleanup the coordinator while stopping the task | SKIPPED | - | - | Already in target via upstream | - | 08b56785fe via Message match (DBZ-9617) |
| 63 | 2e832e3d5d | CC-37278 Backport changes from DBZ-9549 in v2 (#213) | SKIPPED | - | - | Already in target via upstream | - | 355bf3bf7b via DBZ ticket (DBZ-9549) |
| 64 | 80aec8d766 | Add Azure Entra ID credential provider support for SQL Server connector (#211) | CLEAN | - | - | - | 37ee342885 | - |
| 65 | e9b0cb0b44 | Fix IndexOutOfBoundsException for PostgreSQL tables with case-sensitive duplicate | CLEAN | - | - | - | 08aad71305 | - |
| 66 | 074592e878 | Enable checkstyle in semaphore test tasks (#217) | RESOLVED | TRIVIAL | CommonConnectorConfig.java, PostgresEndToEndPerf.java | Removed duplicate field insertion; added ThreadNameContext to PostgresConnection test call | 44e42fcca1 | - |
| 67 | 6b6a43db37 | Add ThreadNameContext Naming for new Snapshot Connector Threads (#218) | RESOLVED | TRIVIAL | BinlogSnapshotChangeEventSource.java | Kept target's lockKeepAliveExecutor/binlogConnectionMutex + added ThreadNameContext; removed Executors import, kept ScheduledExecutorService/TimeUnit | 0a9703cef7 | - |
| 68 | 4c6174a5ae | Revert "Fix IndexOutOfBoundsException for PostgreSQL tables with case-sensitive" | CLEAN | - | - | - | 64ec52860c | - |
| 69 | 3fbbdc62ce | CC-37129: DBZ-9427: Validate guardrail against all tables for schema-history | SKIPPED | - | - | Already in target via upstream | - | a920481423 via DBZ ticket (DBZ-9427) |
| 70 | f47ac9f1e8 | [V2 Snapshot phase] Fix IndexOutOfBoundsException for PostgreSQL tables | CLEAN | - | - | - | 06c4fa8b9a | - |
| 71 | 2c385271ce | CC-37771 Log a warning about binlog retention (#249) | CLEAN | - | - | - | 97c5bc7be7 | - |
| 72 | e81d5b4c3a | CC-37752 - Cherry-pick DBZ-9564 (#251) | SKIPPED | - | - | Already in target via upstream | - | ee10d49304 via DBZ ticket (DBZ-9564) |
| 73 | 0171d3aeee | Compilation fixes | CLEAN | - | - | - | 93aea95937 | - |
| 74 | 8022253c1a | compilation issues | CLEAN | - | - | - | bfb8a587dd | - |
| 75 | 2f1bf541f1 | more build fixes | CLEAN | - | - | - | 8399a56918 | - |
| 76 | dbf1892869 | more fixes in Mysql | CLEAN | - | - | - | f9da01466f | - |
| 77 | 56e25ef873 | Try Removeing condition because it does not work | CLEAN | - | - | - | 3f72371497 | - |
| 78 | 6d9b781608 | fixes OpenLineageIT | CLEAN | - | - | - | 9677ff321f | - |
| 79 | ef42454d24 | Bugfix: Apply duplicate column validation post table filter in Postgres (#257) | CLEAN | - | - | - | f1c3dc9234 | - |
| 80 | 79b4307170 | Bump package versions for CONMON CVE fix in Debezium Postgres Connector (#259) | RESOLVED | TRIVIAL | pom.xml, testcontainers/pom.xml | Added netty 4.1.129, kept target zstd-jni 1.5.6-10; added version.okio property | 46d343592e | - |
| 81 | b01e4d6579 | CC-38136 Fail fast, throw error, when slot is already active (#258) | CLEAN | - | - | - | fda4c9b87c | - |
| 82 | 5975164864 | Reapply "CC-37603: Introduce new ConnectTaskRebalanceExempt Metric MBean" | CLEAN | - | - | - | 33377ed4e1 | - |
| 83 | 06ebb24ffe | CC-38244 Fix NPE when capturing incremental snapshot (#263) | CLEAN | - | - | - | 1673d95af1 | - |
| 84 | f90e89c183 | Reverts schema version change (#265) | CLEAN | - | - | - | 9c10822682 | - |
| 85 | 13bf057bb1 | Updates SnapshotSkipped metrics to use integer, changes extended head (#264) | CLEAN | - | - | - | 1c661c8fff | - |
| 86 | c21db9e9e0 | Adds deprecated fields of SMT back (#267) | CLEAN | - | - | - | 94f18be3b5 | - |
| 87 | c4b3e18cbb | CC-38838: Remove sensitive logging of signalRecord (#275) (#276) | CLEAN | - | - | - | 9269abcab8 | - |
| 88 | 91160b6119 | DBZ-9504 Always return NoOpLineageEmitter when open lineage integration (#277) | SKIPPED | - | - | Already in target via upstream | - | f557464ac2 via DBZ ticket (DBZ-9504) |
| 89 | 31375714e9 | CC-39338: Changes for Mongodb source connector (#334) | CLEAN | - | - | - | 6c86bb43d4 | - |
| 90 | 5851ad8fce | DBZ-9719: Changes for fixing mongo db connector null ref. (#336) | CLEAN | - | - | - | 9bd18e2d38 | - |
| 91 | 266982cd26 | Bump kotlin & commons package versions for CONMON CVE fix in Debezium Postgres | RESOLVED | TRIVIAL | postgres/pom.xml, testcontainers/pom.xml | Added commons-lang3 dep (not mockito); added version.kotlin property | 281567dd8b | - |
| 92 | b7c5d5e28c | Cherrypick from 3.0.8 hotfix x (#346) | CLEAN | - | - | - | 4ba6c05644 | - |
| 93 | e5b8679016 | Fixes changes missed by commit id 2496087 (#347) | SKIPPED | - | - | Empty cherry-pick (already in target) | - | - |
| 94 | 07afd3a59d | Removes wasm/go from test reosurces to prevent release of submodule (#350) | CLEAN | - | - | - | 36b014fe8b | - |
