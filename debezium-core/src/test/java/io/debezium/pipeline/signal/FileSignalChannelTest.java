/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.signal.channels.FileSignalChannel;

/**
 * @author Ismail Simsek
 *
 */
public class FileSignalChannelTest {

    Path signalsData = Paths.get("src", "test", "resources").resolve("debezium_signaling_file.signals.txt");
    Path signalsFile = Paths.get("src", "test", "resources").resolve("debezium_signaling_file.txt");

    @Test
    public void shouldLoadFileSignalsTest() throws IOException {
        Files.copy(signalsData, signalsFile, StandardCopyOption.REPLACE_EXISTING);

        final FileSignalChannel fileSignalChannel = new FileSignalChannel();
        fileSignalChannel.init(config());
        List<SignalRecord> signalRecords = fileSignalChannel.read();
        // only two whitespace lines are ignored
        assertThat(signalRecords).hasSize(2);
        assertThat(signalRecords.get(0).getData().contains("public.MyFirstTable")).isTrue();
        Files.deleteIfExists(signalsFile.toAbsolutePath());
    }

    @Test
    public void shouldNotLogSignalDataOrParseException() throws IOException {
        Files.write(signalsFile, List.of(
                "{\"id\":\"sig-1\",\"type\":\"log\",\"data\":\"{\\\"message\\\":\\\"THIS-IS-CUSTOMER-DATA\\\"}\"}",
                "not-a-json-line-with-CUSTOMER-SECRET"));

        final FileSignalChannel fileSignalChannel = new FileSignalChannel();
        fileSignalChannel.init(config());

        final LogInterceptor log = new LogInterceptor(FileSignalChannel.class);

        List<SignalRecord> signalRecords = fileSignalChannel.read();
        assertThat(signalRecords).hasSize(1);

        // The successfully processed signal's data/additionalData must not be logged: only id + type.
        assertThat(log.containsMessage("Processing signal: sig-1, log")).isTrue();
        assertThat(log.containsMessage("THIS-IS-CUSTOMER-DATA")).isFalse();

        // The unparseable line must not be echoed, nor its parse exception attached: only a fixed
        // reason and the exception class name are safe to report.
        assertThat(log.containsMessage("Skipped signal due to a parse error (JsonParseException)")).isTrue();
        assertThat(log.containsMessage("CUSTOMER-SECRET")).isFalse();
        assertThat(log.containsThrowableWithCause(JsonProcessingException.class)).isFalse();

        Files.deleteIfExists(signalsFile.toAbsolutePath());
    }

    protected CommonConnectorConfig config() {
        return new CommonConnectorConfig(Configuration.create()
                .with(FileSignalChannel.SIGNAL_FILE, signalsFile.toString())
                .build(), 0) {
            @Override
            protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
                return null;
            }

            @Override
            public String getContextName() {
                return null;
            }

            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public EnumeratedValue getSnapshotMode() {
                return null;
            }

            @Override
            public Optional<EnumeratedValue> getSnapshotLockingMode() {
                return Optional.empty();
            }
        };
    }
}
