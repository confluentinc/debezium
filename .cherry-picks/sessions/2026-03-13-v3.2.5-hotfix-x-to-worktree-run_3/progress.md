# Cherry-Pick Progress: v3.2.5-hotfix-x → worktree-run_3

- **Source:** v3.2.5-hotfix-x
- **Target:** worktree-run_3 (based on v3.3.2.Final)
- **Session:** 2026-03-13
- **Review Mode:** End-of-session
- **Sync Tag:** debezium-sync (52d4dbd3d0)

## Summary
- **Total:** 94
- **SKIP (upstream):** 9
- **CLEAN:** 66
- **RESOLVED:** 9
- **SKIPPED (empty/already applied):** 5
- **NOT PICKED (upstream):** 9
- **Working branch commits:** 80

## Progress

| # | SHA | Subject | Status | Complexity | Conflicts | Resolution Summary | Result Commit |
|---|-----|---------|--------|------------|-----------|-------------------|---------------|
| 1 | c5e6da03e6 | Remove unwanted modules | CLEAN | - | - | - | a4506896cd |
| 2 | 8608898351 | Add Semaphore CI | CLEAN | - | - | - | 00553af985 |
| 3 | dfbf907156 | Downgrade maven version | CLEAN | - | - | - | bdc5999d16 |
| 4 | eced94ce1e | Use pgoutput decoder in build profile | CLEAN | - | - | - | ae1b74d4c2 |
| 5 | e4166076b5 | CC-32941 -- Fix MySQL CI | CLEAN | - | - | - | 26c4c38c97 |
| 6 | b23e2dea11 | Ignore flaky test in mysql | CLEAN | - | - | - | 9815508fd6 |
| 7 | 6a2f831990 | Remove logging for configs | CLEAN | - | - | - | 3aff054b74 |
| 8 | e100683437 | Mask CCloud Kafka secrets in logs | CLEAN | - | - | - | 29dd15aef9 |
| 9 | cbb15be133 | Update dependency | CLEAN | - | - | - | a30f625e46 |
| 10 | cf2fb50183 | Validate server version for pgoutput plugin | CLEAN | - | - | - | 8fef5454c2 |
| 11 | 1c2f288b7c | Handle exception thrown in validation | CLEAN | - | - | - | 69b90bdac4 |
| 12 | 2118e9d6f6 | Sanitize AuthenticationException error messages | CLEAN | - | - | - | 77383b1d90 |
| 13 | 11d74ab00f | Convert Debezium Snapshot & Schema Metrics to integer | CLEAN | - | - | - | 8942966bb4 |
| 14 | 4d2ccac6d7 | Fail task if no tables to capture | RESOLVED | TRIVIAL | CommonConnectorConfig.java | Keep both guardrail + failOnNoTables | 959dd7cfd3 |
| 15 | 9e14700759 | Validate connection without checking errors on topic.prefix | CLEAN | - | - | - | 0241ba4875 |
| 16 | cce418286a | V2 wrappers and shading | CLEAN | - | - | - | 5d5633eb64 |
| 17 | 285eaac5f1 | Move commitOffset from OffsetCommiter to task-thread | CLEAN | - | - | - | f3291e8ea1 |
| 18 | 4fc9cfe875 | Return delete.tombstone.handling.mode from ExtractNewRecordState | SKIPPED | - | - | Already in upstream (DBZ-8776) | - |
| 19 | f2dcceb964 | Use re2j in EventRouter SMT | RESOLVED | TRIVIAL | StringsTest.java | Keep Function import, remove java.util.regex.Pattern | ffc07f78e1 |
| 20 | 34b097a95f | Exclude non-required dependencies | CLEAN | - | - | - | 22c37b7fee |
| 21 | 4e1980df8d | Add logs when config value error | CLEAN | - | - | - | ba934ae4ba |
| 22 | c952a4e1d1 | Update condition check on server id | CLEAN | - | - | - | 3320f32c0c |
| 23 | af55e7ac97 | Define limits on regex string lengths | CLEAN | - | - | - | 933f853bf1 |
| 24 | db613ced1c | Fix sensitive data logging for postgres | CLEAN | - | - | - | 48b548ca06 |
| 25 | efb6f872ad | Fix sensitive data logging for mysql | RESOLVED | TRIVIAL | BinlogStreamingChangeEventSource.java | Keep TRUNCATE pattern, apply debug→trace | 67c61686e4 |
| 26 | 718cf524da | Fix lint and formatting | CLEAN | - | - | - | 3515d8f816 |
| 27 | d18ec28d76 | Re-enable additional-condition for backward compat | CLEAN | - | - | - | df1cd7bd0d |
| 28 | 5d22428197 | Fix sensitive data logging for sqlserver | CLEAN | - | - | - | bf479dd1fc |
| 29 | 258c2e12de | Migrate Ubuntu to fix Semaphore build | CLEAN | - | - | - | dc59a5fc08 |
| 30 | 7e65e1f4a2 | Fix lint and import suggestions | SKIPPED | - | - | Empty cherry-pick, already applied | - |
| 31 | 7ae9d5be33 | IAM AssumeRole support in MySQL CDC | CLEAN | - | - | - | 3e99884da3 |
| 32 | a14b23751a | Fix compile failure for common mvn build | CLEAN | - | - | - | 836a9d21d2 |
| 33 | 1d610c5bda | Add mariadb grammar statements back to mysql | CLEAN | - | - | - | cc93989e55 |
| 34 | bf6fa53a1d | Remove support for ts_us and ts_ns | CLEAN | - | - | - | 470d052e94 |
| 35 | 11db8ed7ac | Adding mariadb to service.yml and semaphore.yml | CLEAN | - | - | - | 988581f2b4 |
| 36 | 8c8d796c8c | Perform regex match only in some cases | CLEAN | - | - | - | 61fa451065 |
| 37 | 91a3e94903 | Remove column name from warn log | CLEAN | - | - | - | 1eabad8054 |
| 38 | bb0c076cfa | Prevent trigger,function,view,procedure from history | RESOLVED | TRIVIAL | BinlogStreamingChangeEventSource.java, BinlogRegressionIT.java | Keep both DDL patterns, keep target comments | d5a7550acf |
| 39 | 31437dfaff | Refactor avoid certain statements from history | RESOLVED | MODERATE | BinlogStreamingChangeEventSource.java, SchemaHistory.java | Remove DDL_SKIP_PATTERN, keep target superset filters | c1cede24bc |
| 40 | 210b5f979d | DBZ-9158: publication change | SKIPPED | - | - | Already in upstream (DBZ-9158) | - |
| 41 | 9ab6be6a00 | Expose threadIds of new threads | RESOLVED | MODERATE | MariaDb/MySQL/PostgresConnectorTask.java, CommonConnectorConfig.java | Keep HeartbeatFactory API, add CONNECTOR_THREAD_NAME_PATTERN + getConnectorTaskId | a126f39d10 |
| 42 | f548889260 | Fix cached prepared statement in emitWindowOpen | CLEAN | - | - | - | 5235c4975c |
| 43 | d4a8de1884 | Add ThreadNameContext to PostgresEventStreamingSource | SKIPPED | - | - | Empty cherry-pick, target already has lsnFlushExecutor | - |
| 44 | 66ca661c06 | Validation ConfigDef Injection | CLEAN | - | - | - | ce16a2aa88 |
| 45 | ef98e0450b | Validation ConfigDef Injection (follow-up) | SKIPPED | - | - | Empty cherry-pick | - |
| 46 | 2496087887 | Publication Timeouts | RESOLVED | TRIVIAL | PostgresReplicationConnection.java | Keep target's executeWithTimeout() helper | 108446a343 |
| 47 | be66546aa1 | Fixes 'not connector specific implementation' warn | CLEAN | - | - | - | 0a0611c1e9 |
| 48 | 6fa4258054 | Connector failing to restart due to special table | SKIPPED | - | - | Empty cherry-pick | - |
| 49 | d6aee1df97 | Fix thread naming error during fail-fast validation | CLEAN | - | - | - | 079b471cd3 |
| 50 | 8324e0594d | DBZ-9395: Selectively call ALTER PUBLICATION | SKIPPED | - | - | Already in upstream (DBZ-9395) | - |
| 51 | 174c53436f | Memory should relinquished after task is stopped | CLEAN | - | - | - | 37f26cff6a |
| 52 | 388e28d1a6 | Adds a sleep in the SQL Server IT | CLEAN | - | - | - | abe8844a0b |
| 53 | 4a7f0b1217 | Adds IT for log position validation in SQL Server | CLEAN | - | - | - | 448c6b15ab |
| 54 | 2a10819175 | Add Credential Provider support for Postgres | CLEAN | - | - | - | 90bf872f11 |
| 55 | b6bcc4913c | Bump Credential Provider Version | CLEAN | - | - | - | f9b1ae7cbc |
| 56 | e7ea34166d | DBZ-9427: Introduce guardrail to limit tables | SKIPPED | - | - | Already in upstream (DBZ-9427) | - |
| 57 | dd947575bd | Remove the LsnSeen cache | CLEAN | - | - | - | 05146eeadc |
| 58 | 1b49e6c4e0 | Timezone Validation Error Fix | CLEAN | - | - | - | 01432c1fef |
| 59 | 5cee5df339 | Add read access verification for schema history topic | CLEAN | - | - | - | 144644a11f |
| 60 | 9ddaef7f4c | Fix log position validation bug | CLEAN | - | - | - | f26a7d2a66 |
| 61 | 412bd87810 | Verify schema history read access only for historized | CLEAN | - | - | - | a9113ce865 |
| 62 | 1fc3a307e2 | Gracefully cleanup coordinator while stopping task | SKIPPED | - | - | Already in upstream (DBZ-9617) | - |
| 63 | 2e832e3d5d | Backport changes from DBZ-9549 | SKIPPED | - | - | Already in upstream (DBZ-9549) | - |
| 64 | 80aec8d766 | Azure Entra ID credential provider for SQL Server | CLEAN | - | - | - | 1a4a60937a |
| 65 | e9b0cb0b44 | Fix IndexOutOfBoundsException for PostgreSQL tables | CLEAN | - | - | - | 97e2e2d0b9 |
| 66 | 074592e878 | Enable checkstyle in semaphore test tasks | RESOLVED | TRIVIAL | CommonConnectorConfig.java, PostgresEndToEndPerf.java | Remove duplicate config entries, keep target constructor | f6ffa8433e |
| 67 | 6b6a43db37 | Add ThreadNameContext for new Snapshot Threads | RESOLVED | TRIVIAL | BinlogSnapshotChangeEventSource.java | Keep target imports + fields, add ThreadNameContext | e6a6300bc2 |
| 68 | 4c6174a5ae | Revert Fix IndexOutOfBoundsException | CLEAN | - | - | - | c16ce78482 |
| 69 | 3fbbdc62ce | DBZ-9427: Validate guardrail against all tables | SKIPPED | - | - | Already in upstream (DBZ-9427) | - |
| 70 | f47ac9f1e8 | Fix IndexOutOfBoundsException (V2 Snapshot phase) | CLEAN | - | - | - | 404fd390a7 |
| 71 | 2c385271ce | Log warning about binlog retention | CLEAN | - | - | - | 465cd965ac |
| 72 | e81d5b4c3a | Cherry-pick DBZ-9564 | SKIPPED | - | - | Already in upstream (DBZ-9564) | - |
| 73 | 0171d3aeee | Compilation fixes | CLEAN | - | - | - | b8154a7154 |
| 74 | 8022253c1a | compilation issues | CLEAN | - | - | - | 766a337336 |
| 75 | 2f1bf541f1 | more build fixes | CLEAN | - | - | - | 96e60bad37 |
| 76 | dbf1892869 | more fixes in Mysql | CLEAN | - | - | - | f44e22740c |
| 77 | 56e25ef873 | Try Removeing condition because it does not work | CLEAN | - | - | - | a2f1e21b7e |
| 78 | 6d9b781608 | fixes OpenLineageIT | CLEAN | - | - | - | e0b99269bf |
| 79 | ef42454d24 | Apply duplicate column validation post table filter | CLEAN | - | - | - | 3406e09f41 |
| 80 | 79b4307170 | Bump package versions for CONMON CVE fix | RESOLVED | TRIVIAL | pom.xml, testcontainers/pom.xml | Keep target versions, add okio property | 5737f8d419 |
| 81 | b01e4d6579 | Fail fast when slot is already active | CLEAN | - | - | - | b4777f6c37 |
| 82 | 5975164864 | Reapply ConnectTaskRebalanceExempt Metric MBean | CLEAN | - | - | - | 64b383ff17 |
| 83 | 06ebb24ffe | Fix NPE when capturing incremental snapshot | CLEAN | - | - | - | e3e2a96f1d |
| 84 | f90e89c183 | Reverts schema version change | CLEAN | - | - | - | 79d82c675f |
| 85 | 13bf057bb1 | Updates SnapshotSkipped metrics to use integer | CLEAN | - | - | - | acc22e5e03 |
| 86 | c21db9e9e0 | Adds deprecated fields of SMT back | CLEAN | - | - | - | 6ac951037e |
| 87 | c4b3e18cbb | Remove sensitive logging of signalRecord | CLEAN | - | - | - | 04ec52e3ae |
| 88 | 91160b6119 | DBZ-9504 NoOpLineageEmitter | SKIPPED | - | - | Already in upstream (DBZ-9504) | - |
| 89 | 31375714e9 | Changes for Mongodb source connector | CLEAN | - | - | - | de83e20d9a |
| 90 | 5851ad8fce | Fixing mongo db connector null ref | CLEAN | - | - | - | 97386c7a87 |
| 91 | 266982cd26 | Bump kotlin & commons for CONMON CVE fix | RESOLVED | TRIVIAL | postgres/pom.xml, testcontainers/pom.xml | Add mockito, commons-lang3, kotlin version | 43d64eeb72 |
| 92 | b7c5d5e28c | Cherrypick from 3.0.8 hotfix x | CLEAN | - | - | - | 1811454603 |
| 93 | e5b8679016 | Fixes changes missed by commit id 2496087 | SKIPPED | - | - | Empty cherry-pick | - |
| 94 | 07afd3a59d | Removes wasm/go from test resources | CLEAN | - | - | - | 59837b50f2 |
