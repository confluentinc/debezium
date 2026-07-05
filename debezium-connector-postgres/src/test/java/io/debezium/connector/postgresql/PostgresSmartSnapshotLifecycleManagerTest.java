/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

public class PostgresSmartSnapshotLifecycleManagerTest {

    @SuppressWarnings("unchecked")
    private PostgresSmartSnapshotLifecycleManager newManager() {
        return new PostgresSmartSnapshotLifecycleManager(
                mock(PostgresConnectorConfig.class),
                mock(MainConnectionProvidingConnectionFactory.class),
                mock(PostgresTaskContext.class),
                mock(SnapshotterService.class),
                mock(PostgresSchema.class),
                mock(EventDispatcher.class),
                mock(NotificationService.class),
                Clock.system());
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = PostgresSmartSnapshotLifecycleManager.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    // releaseSnapshot() (called from the task-stop path) and keepAlive()
    // (called from the leader prep thread) touch the same held connections and must be mutually exclusive.
    // We block releaseSnapshot inside its critical section (the held connection's close() blocks) and assert
    // keepAlive cannot run until it is released.
    @Test
    public void releaseSnapshotAndKeepAliveAreMutuallyExclusive() throws Exception {
        PostgresSmartSnapshotLifecycleManager manager = newManager();

        PostgresConnection held = mock(PostgresConnection.class);
        CountDownLatch inClose = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        doAnswer(inv -> {
            inClose.countDown();
            proceed.await();
            return null;
        }).when(held).close();

        setField(manager, "snapshotHolderConnection", held);

        Thread releaser = new Thread(manager::releaseSnapshot, "releaser");
        releaser.start();
        // releaseSnapshot holds the monitor, stuck in close()
        assertThat(inClose.await(5, TimeUnit.SECONDS)).isTrue();

        AtomicBoolean keepAliveReturned = new AtomicBoolean(false);
        Thread pinger = new Thread(() -> {
            manager.keepAlive();
            keepAliveReturned.set(true);
        }, "pinger");
        pinger.start();

        Thread.sleep(300);
        // excluded while releaseSnapshot holds the lock
        assertThat(keepAliveReturned).isFalse();

        proceed.countDown();
        releaser.join(2000);
        pinger.join(2000);
        assertThat(keepAliveReturned).isTrue();
    }
}
