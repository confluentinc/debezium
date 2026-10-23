/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TEMPORARY DEBUG HELPER (branch kdatta/query_timeout_debug_logs_patch1).
 *
 * <p>Wraps timed JDBC query/statement execution so that during canary testing we can observe
 * {@code query.timeout.ms} behaviour end to end. For every wrapped call it logs:
 * <ul>
 *   <li>an INFO {@code START} line (label, configured timeout in seconds, thread, SQL text) before execution;</li>
 *   <li>an INFO {@code SUCCESS} line (label, elapsed ms) when the call returns normally;</li>
 *   <li>an INFO {@code SQL-FAILURE} line (label, elapsed ms, SQLSTATE, vendor error code, exception class,
 *       message and full stack trace) when a {@link SQLException} is thrown.</li>
 * </ul>
 * The elapsed time lets us confirm whether a stuck query actually unblocks at {@code query.timeout.ms}
 * (default 600s) or earlier/later, and the SQLSTATE/message lets us error-map the cancellation exception.
 *
 * <p>This wrapper is behavior-transparent: on failure it logs and re-throws the identical {@link SQLException},
 * and on success it returns the same {@link ResultSet}. It only observes; it does not alter control flow.
 *
 * <p>NOT for production. Remove before merging to a release branch.
 */
public final class QueryTimeoutDebug {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryTimeoutDebug.class);

    private QueryTimeoutDebug() {
    }

    public static ResultSet executeQuery(PreparedStatement stmt, String label, String sql, int timeoutSeconds) throws SQLException {
        final long start = logStart(label, sql, timeoutSeconds);
        try {
            final ResultSet rs = stmt.executeQuery();
            logSuccess(label, start, timeoutSeconds);
            return rs;
        }
        catch (SQLException e) {
            logFailure(label, start, timeoutSeconds, e);
            throw e;
        }
    }

    public static ResultSet executeQuery(Statement stmt, String label, String sql, int timeoutSeconds) throws SQLException {
        final long start = logStart(label, sql, timeoutSeconds);
        try {
            final ResultSet rs = stmt.executeQuery(sql);
            logSuccess(label, start, timeoutSeconds);
            return rs;
        }
        catch (SQLException e) {
            logFailure(label, start, timeoutSeconds, e);
            throw e;
        }
    }

    public static boolean execute(Statement stmt, String label, String sql, int timeoutSeconds) throws SQLException {
        final long start = logStart(label, sql, timeoutSeconds);
        try {
            final boolean result = stmt.execute(sql);
            logSuccess(label, start, timeoutSeconds);
            return result;
        }
        catch (SQLException e) {
            logFailure(label, start, timeoutSeconds, e);
            throw e;
        }
    }

    private static long logStart(String label, String sql, int timeoutSeconds) {
        LOGGER.info("[QT-DEBUG] START label='{}' timeoutSeconds={} thread='{}' sql=[{}]",
                label, timeoutSeconds, Thread.currentThread().getName(), sql);
        return System.nanoTime();
    }

    private static void logSuccess(String label, long startNanos, int timeoutSeconds) {
        LOGGER.info("[QT-DEBUG] SUCCESS label='{}' elapsedMs={} timeoutSeconds={}",
                label, elapsedMs(startNanos), timeoutSeconds);
    }

    private static void logFailure(String label, long startNanos, int timeoutSeconds, SQLException e) {
        LOGGER.info("[QT-DEBUG] SQL-FAILURE label='{}' elapsedMs={} timeoutSeconds={} sqlState={} errorCode={} exceptionClass={} message={}",
                label, elapsedMs(startNanos), timeoutSeconds, e.getSQLState(), e.getErrorCode(), e.getClass().getName(), e.getMessage(), e);
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
