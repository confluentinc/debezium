/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import static org.junit.Assert.fail;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

/**
 * DBZ-9558 best-effort reproducer (DO NOT MERGE).
 *
 * <p>On JDK 23+ the C2 JIT can miscompile {@link java.time.chrono.ChronoLocalDateTime#toEpochSecond()}
 * so that {@code toLocalDate()} spuriously returns {@code null}, producing:
 *
 * <pre>
 * java.lang.NullPointerException: Cannot invoke "java.time.chrono.ChronoLocalDate.toEpochDay()"
 *   because the return value of "java.time.chrono.ChronoLocalDateTime.toLocalDate()" is null
 *     at java.base/java.time.chrono.ChronoLocalDateTime.toEpochSecond(...)
 *     at io.debezium.time.Timestamp.toEpochMillis(Timestamp.java:77)
 * </pre>
 *
 * The upstream bug (DBZ-9558) is non-deterministic and was only ever observed on x86-64 CI, never
 * locally and never on arm64. This test brute-forces the exact conversion methods
 * ({@link Timestamp#toEpochMillis} / {@link MicroTimestamp#toEpochMicros}) plus the raw JDK path
 * from the stack trace, across all available cores for a long time, to give C2 the chance to reach
 * the miscompiled state. If the NPE fires, the test fails and captures the environment.
 *
 * This is a probabilistic reproducer, not a deterministic assertion: a green run does NOT prove the
 * bug is absent, it only means C2 did not miscompile within the iteration budget on this run.
 */
public class Dbz9558Jdk24TimestampNpeReproTest {

    private static final int THREADS = Math.max(4, Runtime.getRuntime().availableProcessors());
    private static final long ITERATIONS_PER_THREAD = 150_000_000L;

    private static volatile long SINK;

    @Test
    public void hammerTimestampConversionForJdk23Npe() throws Exception {
        // reporter's value: 1989-04-21 00:00:00.0
        final long base = java.sql.Timestamp.valueOf("1989-04-21 00:00:00.0").getTime();
        final AtomicReference<Throwable> firstFailure = new AtomicReference<>();

        System.out.println("[DBZ-9558] hammering " + THREADS + " threads x " + ITERATIONS_PER_THREAD
                + " iters on java " + System.getProperty("java.version")
                + " (" + System.getProperty("java.vm.name") + "), arch=" + System.getProperty("os.arch"));

        final Thread[] workers = new Thread[THREADS];
        for (int t = 0; t < THREADS; t++) {
            workers[t] = new Thread(() -> {
                long acc = 0L;
                try {
                    for (long i = 0; i < ITERATIONS_PER_THREAD && firstFailure.get() == null; i++) {
                        // vary the value so it is not constant-folded; keep it a valid timestamp
                        final java.sql.Timestamp sql = new java.sql.Timestamp(base + (i & 0xFFFF));

                        // millis path: JdbcValueConverters -> Timestamp.toEpochMillis (precision <= 3)
                        acc ^= Timestamp.toEpochMillis(sql, null);
                        // micros path: JdbcValueConverters -> MicroTimestamp.toEpochMicros (precision 4-6)
                        acc ^= MicroTimestamp.toEpochMicros(sql, null);
                        // raw JDK frames from the reported stack: LocalDateTime -> Instant
                        final LocalDateTime ldt = sql.toLocalDateTime();
                        acc ^= ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
                    }
                }
                catch (Throwable ex) {
                    firstFailure.compareAndSet(null, ex);
                }
                SINK += acc; // publish so the JIT cannot dead-code-eliminate the loop body
            }, "dbz9558-hammer-" + t);
            workers[t].start();
        }
        for (Thread w : workers) {
            w.join();
        }

        final Throwable failure = firstFailure.get();
        if (failure != null) {
            // If this is the ChronoLocalDateTime.toLocalDate() NPE, DBZ-9558 reproduced on this env.
            failure.printStackTrace();
            fail("DBZ-9558 REPRODUCED on java " + System.getProperty("java.version")
                    + " arch=" + System.getProperty("os.arch") + " sink=" + SINK + " : " + failure);
        }
        System.out.println("[DBZ-9558] no NPE this run (sink=" + SINK + "); C2 did not miscompile within budget");
    }
}
