/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
                Clock.system(), 1);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = PostgresSmartSnapshotLifecycleManager.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    // On the fixed design releaseSnapshot must not wait for a long-running operation that another
    // thread is doing on a held connection. Here keepAlive is blocked inside its ping; releaseSnapshot
    // on another thread must still finish quickly. On the old design (all methods synchronized on the
    // same monitor) releaseSnapshot would block until the ping returned, and this test would time out.
    @Test
    public void releaseSnapshotDoesNotWaitForInFlightKeepAlive() throws Exception {
        PostgresSmartSnapshotLifecycleManager manager = newManager();

        PostgresConnection held = mock(PostgresConnection.class);
        CountDownLatch pinging = new CountDownLatch(1);
        CountDownLatch letPingFinish = new CountDownLatch(1);
        // keepAlive pings the held connection with "SELECT 1"; make that call block to model a long
        // in-flight operation on the connection.
        doAnswer(inv -> {
            pinging.countDown();
            letPingFinish.await(5, TimeUnit.SECONDS);
            return null;
        }).when(held).executeWithoutCommitting("SELECT 1");

        setField(manager, "snapshotHolderConnection", held);

        Thread pinger = new Thread(manager::keepAlive, "pinger");
        pinger.start();
        // wait until keepAlive is blocked inside the ping
        assertThat(pinging.await(5, TimeUnit.SECONDS)).isTrue();

        // releaseSnapshot on another thread must finish promptly, not wait for the blocked ping.
        Thread releaser = new Thread(manager::releaseSnapshot, "releaser");
        releaser.start();
        releaser.join(2000);
        try {
            assertThat(releaser.isAlive()).isFalse();
        }
        finally {
            letPingFinish.countDown();
            pinger.join(2000);
        }
    }

    // releaseSnapshot may be called from both the prep thread and the stop thread; it must close each
    // held connection exactly once.
    @Test
    public void releaseSnapshotIsIdempotent() throws Exception {
        PostgresSmartSnapshotLifecycleManager manager = newManager();
        PostgresConnection held = mock(PostgresConnection.class);
        setField(manager, "snapshotHolderConnection", held);

        manager.releaseSnapshot();
        manager.releaseSnapshot();

        verify(held, times(1)).close();
    }

    // After a release the held connections are gone. keepAlive must be a safe no-op: it must not throw
    // and must not ping a connection that has already been closed.
    @Test
    public void keepAliveAfterReleaseDoesNothing() throws Exception {
        PostgresSmartSnapshotLifecycleManager manager = newManager();
        PostgresConnection held = mock(PostgresConnection.class);
        setField(manager, "snapshotHolderConnection", held);

        manager.releaseSnapshot();
        manager.keepAlive();

        verify(held, never()).executeWithoutCommitting("SELECT 1");
    }
}
