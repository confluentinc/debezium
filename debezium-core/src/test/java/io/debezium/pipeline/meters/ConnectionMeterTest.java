/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ConnectionMeterTest {

    @Test
    public void shouldBeDisconnectedByDefault() {
        ConnectionMeter meter = new ConnectionMeter();
        assertThat(meter.getConnected()).isEqualTo(0L);
    }

    @Test
    public void shouldReportOneWhenConnected() {
        ConnectionMeter meter = new ConnectionMeter();
        meter.connected(true);
        assertThat(meter.getConnected()).isEqualTo(1L);
    }

    @Test
    public void shouldReportZeroWhenDisconnected() {
        ConnectionMeter meter = new ConnectionMeter();
        meter.connected(true);
        meter.connected(false);
        assertThat(meter.getConnected()).isEqualTo(0L);
    }

    @Test
    public void shouldReportZeroAfterReset() {
        ConnectionMeter meter = new ConnectionMeter();
        meter.connected(true);
        meter.reset();
        assertThat(meter.getConnected()).isEqualTo(0L);
    }
}
