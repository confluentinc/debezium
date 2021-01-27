/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringStartsWith.startsWith;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Before;
import org.junit.Test;

public class SqlServerConnectorTest {
    SqlServerConnector connector;

    @Before
    public void before() {
        connector = new SqlServerConnector();
    }

    @Test
    public void testValidateUnableToConnectNoThrow() {
        Map<String, String> config = new HashMap<>();
        config.put(SqlServerConnectorConfig.HOSTNAME.name(), "narnia");
        config.put(SqlServerConnectorConfig.PORT.name(), "4321");
        config.put(SqlServerConnectorConfig.DATABASE_NAME.name(), "sqlserver");
        config.put(SqlServerConnectorConfig.USER.name(), "pikachu");
        config.put(SqlServerConnectorConfig.PASSWORD.name(), "raichu");

        Config validated = connector.validate(config);
        for (ConfigValue value : validated.configValues()) {
            if (config.containsKey(value.name())) {
                assertThat(value.errorMessages().get(0), startsWith("Unable to connect:"));
            }
        }
    }
}
