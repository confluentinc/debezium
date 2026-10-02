/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions.snapshotting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.spi.Partition;

/**
 * No-op signal action used by the incremental-snapshot pre-flight probe.
 * The probe row is INSERTed into the signal table BEFORE state mutation in
 * addDataCollectionNamesToSnapshot. If the INSERT fails (permission denied, etc.)
 * the signal is rejected cleanly with no orphan state. If the INSERT succeeds,
 * the probe row is committed and flows through CDC like any other signal — when
 * it lands back at the SignalProcessor it dispatches to this no-op handler.
 */
public class IncrementalSnapshotProbe<P extends Partition> implements SignalAction<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalSnapshotProbe.class);

    public static final String NAME = "incremental-snapshot-probe";

    @Override
    public boolean arrived(SignalPayload<P> signalPayload) {
        LOGGER.trace("Probe signal '{}' received; no action taken (pre-flight probe row only).", signalPayload.id);
        return true;
    }
}
