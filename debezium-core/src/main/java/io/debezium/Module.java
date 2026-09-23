/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium;

import java.util.Properties;

import io.debezium.util.IoUtil;

// No-op touch to trigger Semaphore CI for the netty-resolver-dns/netty-codec-dns BOM pin (VDR-3677, VDR-3706, VDR-3695).
public class Module {

    private static final Properties INFO = IoUtil.loadProperties(Module.class, "io/debezium/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }
}
