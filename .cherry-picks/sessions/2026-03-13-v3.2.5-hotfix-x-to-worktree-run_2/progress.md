# Cherry-Pick Session: v3.2.5-hotfix-x → worktree-run_2
- **Date:** 2026-03-13
- **Source:** v3.2.5-hotfix-x
- **Target:** worktree-run_2 (based on v3.3.2.Final)
- **Upstream tag:** v3.3.2.Final
- **Total commits:** 94
- **Skip (upstream):** 9
- **To pick:** 85

## Summary
- CLEAN: 68
- RESOLVED: 10
- SKIPPED: 13 (9 upstream + 3 empty + 1 empty/duplicate)

## Progress

| # | SHA | Subject | Status | Complexity | Conflicts | Resolution Summary | Result Commit |
|---|-----|---------|--------|------------|-----------|-------------------|---------------|
| 1 | c5e6da03e6 | Remove unwanted modules | CLEAN | - | - | - | 596f2fee91 |
| 2 | 8608898351 | Add Semaphore CI | CLEAN | - | - | - | d4149ce838 |
| 3 | dfbf907156 | Downgrade maven version | CLEAN | - | - | - | b476246f57 |
| 4 | eced94ce1e | Use pgoutput decoder in build profile | CLEAN | - | - | - | 18eb780cab |
| 5 | e4166076b5 | CC-32941 -- Fix MySQL CI | CLEAN | - | - | - | 214034ebbf |
| 6 | b23e2dea11 | Ignore flaky test in mysql | CLEAN | - | - | - | ca9220ed98 |
| 7 | 6a2f831990 | Remove logging for configs | CLEAN | - | - | - | eda8638997 |
| 8 | e100683437 | Mask CCloud Kafka secrets in logs | CLEAN | - | - | - | 7c3ad54f50 |
| 9 | cbb15be133 | Update dependency | CLEAN | - | - | - | 0b60a7bf64 |
| 10 | cf2fb50183 | Validate server version for pgoutput plugin | CLEAN | - | - | - | ba0f8e61d1 |
| 11 | 1c2f288b7c | Handle exception thrown in validation | CLEAN | - | - | - | auto |
| 12 | 2118e9d6f6 | Sanitize AuthenticationException error messages | CLEAN | - | - | - | auto |
| 13 | 11d74ab00f | Convert Debezium Snapshot & Schema Metrics to integer | CLEAN | - | - | - | auto |
| 14 | 4d2ccac6d7 | Fail task if no tables to capture | RESOLVED | MODERATE | CommonConnectorConfig.java | Keep both guardrail fields + failOnNoTables | 5b899d3bf2 |
| 15 | 9e14700759 | Validate connection without checking errors on topic.prefix | CLEAN | - | - | - | 937dce32fd |
| 16 | cce418286a | V2 wrappers and shading | CLEAN | - | - | - | 1d1892fb53 |
| 17 | 285eaac5f1 | Move commitOffset from OffsetCommiter to task-thread | CLEAN | - | - | - | 3ad9803a38 |
| 18 | 4fc9cfe875 | Return delete.tombstone.handling.mode from ExtractNewRecordState | SKIPPED | - | - | Already in target via upstream | - |
| 19 | f2dcceb964 | Use re2j in EventRouter SMT instead of RegexRouter | RESOLVED | TRIVIAL | StringsTest.java | Keep Function import, remove java.util.regex.Pattern | 113acb0a15 |
| 20 | 34b097a95f | Exclude non-required dependencies | CLEAN | - | - | - | 19143f66ae |
| 21 | 4e1980df8d | Add logs when there is config value error | CLEAN | - | - | - | f4b120fe14 |
| 22 | c952a4e1d1 | Update condition check on server id | CLEAN | - | - | - | cf54903fe4 |
| 23 | af55e7ac97 | Define limits on length of strings to be matched with regexes | CLEAN | - | - | - | 7279305627 |
| 24 | db613ced1c | Fix sensitive data logging for debezium postgres | CLEAN | - | - | - | f8516a8c05 |
| 25 | efb6f872ad | Fix sensitive data logging for debezium mysql | RESOLVED | TRIVIAL | BinlogStreamingChangeEventSource.java | Keep TRUNCATE pattern + change debug to trace | 66ad182b3c |
| 26 | 718cf524da | Fix lint and formatting | CLEAN | - | - | - | 9f4f51e892 |
| 27 | d18ec28d76 | Temporarily re-enable additional-condition | CLEAN | - | - | - | fc9ed7fe0f |
| 28 | 5d22428197 | Fix sensitive data logging for debezium sqlserver | CLEAN | - | - | - | c2aa0e3d8a |
| 29 | 258c2e12de | Migrate Ubuntu to fix Semaphore build | CLEAN | - | - | - | 0d84a712ef |
| 30 | 7e65e1f4a2 | Fix lint and import suggestions | SKIPPED | - | - | Empty — changes already applied | - |
| 31 | 7ae9d5be33 | Cherrypick IAM AssumeRole support in MySQL CDC | CLEAN | - | - | - | e3c32d1c3b |
| 32 | a14b23751a | Fix compile failure for common mvn build | CLEAN | - | - | - | f4a89fe44f |
| 33 | 1d610c5bda | Add mariadb grammar statements back to mysql grammar | CLEAN | - | - | - | 242d66aedd |
| 34 | bf6fa53a1d | Remove support for ts_us and ts_ns | CLEAN | - | - | - | 34b27aa8e2 |
| 35 | 11db8ed7ac | Adding mariadb to service.yml and semaphore.yml | CLEAN | - | - | - | bc62b261bb |
| 36 | 8c8d796c8c | Perform regex match only in some cases | CLEAN | - | - | - | e8ee3e06d6 |
| 37 | 91a3e94903 | CC-34563 Remove column name from warn log | CLEAN | - | - | - | 8bbbdd3f36 |
| 38 | bb0c076cfa | Prevent trigger,function,view,procedure from history topic | RESOLVED | MODERATE | BinlogStreamingChangeEventSource.java, BinlogRegressionIT.java | Keep both TRUNCATE+DDL_SKIP patterns, keep target's comment | 5519103501 |
| 39 | 31437dfaff | Refactor avoid certain statements from history topic | RESOLVED | MODERATE | BinlogStreamingChangeEventSource.java, SchemaHistory.java | Remove DDL_SKIP+SET_STATEMENT, keep TRUNCATE; keep target's extra filter patterns | f0c81edab0 |
| 40 | 210b5f979d | DBZ-9158: cherry picking publication change | SKIPPED | - | - | Already in target via upstream | - |
| 41 | 9ab6be6a00 | Expose threadIds of the new threads | RESOLVED | COMPLEX | 4 files | Keep both guardrail+ThreadNameContext fields; keep target HeartbeatFactory API | e077548cbc |
| 42 | f548889260 | Fix usage of cached prepared statement in emitWindowOpen | CLEAN | - | - | - | dcbbd849c2 |
| 43 | d4a8de1884 | Add ThreadNameContext to PostgresEventStreamingSource | RESOLVED | MODERATE | PostgresStreamingChangeEventSource.java | Keep target's lsnFlushExecutor, add ThreadNameContext to its creation | 34e83bbdb9 |
| 44 | 66ca661c06 | Validation ConfigDef Injection | CLEAN | - | - | - | d9e4189eb1 |
| 45 | ef98e0450b | Validation ConfigDef Injection (2nd) | SKIPPED | - | - | Empty — changes already applied | - |
| 46 | 2496087887 | Publication Timeouts | RESOLVED | MODERATE | PostgresReplicationConnection.java | Keep target's executeWithTimeout, keep target's error message | c61e987bc6 |
| 47 | be66546aa1 | Fixes Found a not connector specific implementation warn | CLEAN | - | - | - | a8c8dd655a |
| 48 | 6fa4258054 | Connector failing to restart due to special table | SKIPPED | - | - | Empty — changes already applied | - |
| 49 | d6aee1df97 | Fix thread naming error when connector name is null | CLEAN | - | - | - | 91cb38a5b9 |
| 50 | 8324e0594d | DBZ-9395: Selectively call ALTER PUBLICATION | SKIPPED | - | - | Already in target via upstream | - |
| 51 | 174c53436f | Memory should relinquished after task is stopped | CLEAN | - | - | - | c3ed9a20d5 |
| 52 | 388e28d1a6 | Adds a sleep in SQL Server IT | CLEAN | - | - | - | 9dadeec459 |
| 53 | 4a7f0b1217 | Adds IT for log position validation in SQL Server | CLEAN | - | - | - | 206038b676 |
| 54 | 2a10819175 | Add Credential Provider support for Postgres | CLEAN | - | - | - | 399fbbf058 |
| 55 | b6bcc4913c | Bump Credential Provider Version | CLEAN | - | - | - | 03495900d8 |
| 56 | e7ea34166d | DBZ-9427: Introduce guardrail to limit tables captured | SKIPPED | - | - | Already in target via upstream | - |
| 57 | dd947575bd | Remove the LsnSeen cache | CLEAN | - | - | - | 2109a49381 |
| 58 | 1b49e6c4e0 | Timezone Validation Error Fix | CLEAN | - | - | - | 66443f3b75 |
| 59 | 5cee5df339 | Add read access verification for schema history topic | CLEAN | - | - | - | 224ecf0ffa |
| 60 | 9ddaef7f4c | Fix log position validation bug | CLEAN | - | - | - | 8404a2b978 |
| 61 | 412bd87810 | Verify schema history read access for historized connectors | CLEAN | - | - | - | 19b974d0d0 |
| 62 | 1fc3a307e2 | Gracefully cleanup coordinator while stopping task | SKIPPED | - | - | Already in target via upstream | - |
| 63 | 2e832e3d5d | Backport changes from DBZ-9549 | SKIPPED | - | - | Already in target via upstream | - |
| 64 | 80aec8d766 | Add Azure Entra ID credential provider for SQL Server | CLEAN | - | - | - | fd074c9d9e |
| 65 | e9b0cb0b44 | Fix IndexOutOfBoundsException for PostgreSQL tables | CLEAN | - | - | - | 84f1d4a58b |
| 66 | 074592e878 | Enable checkstyle in semaphore test tasks | RESOLVED | TRIVIAL | CommonConnectorConfig.java, PostgresEndToEndPerf.java | Remove duplicate config entries, accept ThreadNameContext in test | ccbba21bf7 |
| 67 | 6b6a43db37 | Add ThreadNameContext for Snapshot Connector Threads | RESOLVED | MODERATE | BinlogSnapshotChangeEventSource.java | Keep both executor/mutex fields + add ThreadNameContext | 3f8ebae68d |
| 68 | 4c6174a5ae | Revert Fix IndexOutOfBoundsException for PostgreSQL tables | CLEAN | - | - | - | 3415ef542c |
| 69 | 3fbbdc62ce | DBZ-9427: Validate guardrail for all tables | SKIPPED | - | - | Already in target via upstream | - |
| 70 | f47ac9f1e8 | [V2] Fix IndexOutOfBoundsException for PostgreSQL tables | CLEAN | - | - | - | 9577ba9788 |
| 71 | 2c385271ce | Log a warning about binlog retention | CLEAN | - | - | - | 25eb120328 |
| 72 | e81d5b4c3a | Cherry-pick DBZ-9564 | SKIPPED | - | - | Already in target via upstream | - |
| 73 | 0171d3aeee | Compilation fixes | CLEAN | - | - | - | 4fe15d1ce5 |
| 74 | 8022253c1a | compilation issues | CLEAN | - | - | - | 480ec26215 |
| 75 | 2f1bf541f1 | more build fixes | CLEAN | - | - | - | 4823e25a00 |
| 76 | dbf1892869 | more fixes in Mysql | CLEAN | - | - | - | c289742cd2 |
| 77 | 56e25ef873 | Try Removeing condition because it does not work | CLEAN | - | - | - | 1c36e96d64 |
| 78 | 6d9b781608 | fixes OpenLineageIT | CLEAN | - | - | - | 7a266bf73d |
| 79 | ef42454d24 | Apply duplicate column validation post table filter | CLEAN | - | - | - | d5c359f611 |
| 80 | 79b4307170 | Bump package versions for CONMON CVE fix | RESOLVED | MODERATE | pom.xml, testcontainers/pom.xml | Keep target zstd version, bump netty for CVE, add okio property | 2e0df6e750 |
| 81 | b01e4d6579 | Fail fast when slot is already active | CLEAN | - | - | - | cfef6949ae |
| 82 | 5975164864 | Reapply ConnectTaskRebalanceExempt Metric MBean | CLEAN | - | - | - | 85fc0993c6 |
| 83 | 06ebb24ffe | Fix NPE when capturing incremental snapshot | CLEAN | - | - | - | 2fa3db085f |
| 84 | f90e89c183 | Reverts schema version change | CLEAN | - | - | - | ba78c62714 |
| 85 | 13bf057bb1 | Updates SnapshotSkipped metrics to use integer | CLEAN | - | - | - | eaa4bf997a |
| 86 | c21db9e9e0 | Adds deprecated fields of SMT back | CLEAN | - | - | - | 2c2c02ba16 |
| 87 | c4b3e18cbb | Remove sensitive logging of signalRecord | CLEAN | - | - | - | 353ef03170 |
| 88 | 91160b6119 | DBZ-9504 NoOpLineageEmitter when open lineage disabled | SKIPPED | - | - | Already in target via upstream | - |
| 89 | 31375714e9 | Changes for Mongodb source connector | CLEAN | - | - | - | 43ac5de501 |
| 90 | 5851ad8fce | DBZ-9719: Fix mongo db connector null ref | CLEAN | - | - | - | 8323db9870 |
| 91 | 266982cd26 | Bump kotlin & commons for CONMON CVE fix | RESOLVED | MODERATE | postgres/pom.xml, testcontainers/pom.xml | Add mockito, commons-lang3 deps; keep target version; add kotlin property | 9adf98b4a7 |
| 92 | b7c5d5e28c | Cherrypick from 3.0.8 hotfix x | CLEAN | - | - | - | 77e9d91dae |
| 93 | e5b8679016 | Fixes changes missed by commit id 2496087 | SKIPPED | - | - | Empty — changes already applied | - |
| 94 | 07afd3a59d | Removes wasm/go from test resources | CLEAN | - | - | - | 8ee8eb37b1 |
