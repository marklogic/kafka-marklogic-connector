/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbstractSinkTaskTest {

    @Test
    void skipsNullRecordsAndRecordsWithNullValues() {
        RecordingSinkTask task = new RecordingSinkTask();

        task.put(Arrays.asList(null, new SinkRecord("topic", 0, null, null, null, null, 0L),
            new SinkRecord("topic", 0, null, null, null, "value", 1L)));

        assertEquals(1, task.writtenRecords);
    }

    @Test
    void wrapsWriteFailuresWithRecordOffset() {
        RecordingSinkTask task = new RecordingSinkTask();
        task.failOnWrite = true;

        RuntimeException error = assertThrows(RuntimeException.class,
            () -> task.put(Arrays.asList(new SinkRecord("topic", 0, null, null, null, "value", 23L))));

        assertTrue(error.getMessage().contains("record offset: 23"));
        assertTrue(error.getMessage().contains("simulated write failure"));
    }

    @Test
    void logsRecordKeyAndHeadersWhenLoggingIsEnabled() {
        RecordingSinkTask task = new RecordingSinkTask();
        Map<String, String> config = new HashMap<>();
        config.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        config.put(MarkLogicSinkConfig.CONNECTION_PORT, "8000");
        config.put(MarkLogicSinkConfig.LOGGING_RECORD_KEY, "true");
        config.put(MarkLogicSinkConfig.LOGGING_RECORD_HEADERS, "true");
        task.start(config);

        SinkRecord record = new SinkRecord("topic", 0, null, "some-key", null, "value", 0L);
        record.headers().addString("A", "1");
        task.put(Arrays.asList(record));

        assertEquals(1, task.writtenRecords, "Logging must not interfere with writing the record");
    }

    @Test
    void logsRecordValueWhenTraceLoggingIsEnabled() {
        RecordingSinkTask task = new RecordingSinkTask();
        Logger taskLogger = (Logger) LoggerFactory.getLogger(RecordingSinkTask.class);
        Level originalLevel = taskLogger.getLevel();
        taskLogger.setLevel(Level.TRACE);
        try {
            task.put(Arrays.asList(new SinkRecord("topic", 0, null, null, null, "value", 0L)));

            assertEquals(1, task.writtenRecords, "Trace logging must not interfere with writing the record");
        } finally {
            taskLogger.setLevel(originalLevel);
        }
    }

    @Test
    void versionMatchesTheSinkConnectorVersion() {
        assertEquals(MarkLogicSinkConnector.MARKLOGIC_SINK_CONNECTOR_VERSION, new RecordingSinkTask().version());
    }

    private static class RecordingSinkTask extends AbstractSinkTask {
        private int writtenRecords;
        private boolean failOnWrite;

        @Override
        protected void onStart(java.util.Map<String, Object> parsedConfig) {
        }

        @Override
        protected void writeSinkRecord(SinkRecord sinkRecord) {
            if (failOnWrite) {
                throw new IllegalStateException("simulated write failure");
            }
            writtenRecords++;
        }

        @Override
        public void stop() {
        }
    }
}
