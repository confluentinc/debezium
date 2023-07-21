/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.pipeline.notification.AbstractNotificationsIT;

public class NotificationsIT extends AbstractNotificationsIT<PostgresConnector_V2> {

    @Before
    public void before() throws SQLException {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropAllSchemas();
        initializeConnectorTestFramework();
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Override
    protected Class<PostgresConnector_V2> connectorClass() {
        return PostgresConnector_V2.class;
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig_V2.SNAPSHOT_MODE, PostgresConnectorConfig_V2.SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig_V2.DROP_SLOT_ON_STOP, Boolean.TRUE);
    }

    @Override
    protected String connector() {
        return "postgres";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER;
    }

    @Override
    protected String snapshotStatusResult() {
        return "COMPLETED";
    }
}
