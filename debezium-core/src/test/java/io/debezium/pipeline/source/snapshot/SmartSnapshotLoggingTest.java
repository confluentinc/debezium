/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 * Pins the smart snapshot log prefix format. Log searches and dashboards key on it (e.g. {@code role=leader epoch=3}),
 * so a change here should be deliberate.
 */
public class SmartSnapshotLoggingTest {

    @Test
    public void rolesWithAnEpoch() {
        assertThat(SmartSnapshotLogging.task("1", 3)).isEqualTo("Smart snapshot: [role=task taskId=1 epoch=3]");
        assertThat(SmartSnapshotLogging.leader(3)).isEqualTo("Smart snapshot: [role=leader epoch=3]");
        assertThat(SmartSnapshotLogging.monitor(3)).isEqualTo("Smart snapshot: [role=monitor epoch=3]");
        assertThat(SmartSnapshotLogging.connector(3)).isEqualTo("Smart snapshot: [role=connector epoch=3]");
    }

    @Test
    public void rolesWithoutAnEpoch() {
        assertThat(SmartSnapshotLogging.task("1")).isEqualTo("Smart snapshot: [role=task taskId=1]");
        assertThat(SmartSnapshotLogging.TASK).isEqualTo("Smart snapshot: [role=task]");
        assertThat(SmartSnapshotLogging.CONNECTOR).isEqualTo("Smart snapshot: [role=connector]");
        assertThat(SmartSnapshotLogging.COORDINATION).isEqualTo("Smart snapshot: [role=coordination]");
    }
}
