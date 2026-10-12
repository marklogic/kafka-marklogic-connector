/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import com.marklogic.client.datamovement.impl.WriteBatchImpl;
import com.marklogic.client.datamovement.impl.WriteEventImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteFailureHandlerTest {

    @Test
    void reportsFailedSinkRecordAndAddsFailureHeaders() {
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, "value", 5L);
        SinkRecordMetadataHandle metadata = new SinkRecordMetadataHandle(record);
        WriteEventImpl event = new WriteEventImpl().withTargetUri("/failed/5.json").withMetadata(metadata);
        List<SinkRecord> reported = new ArrayList<>();
        List<Throwable> failures = new ArrayList<>();
        WriteFailureHandler handler = new WriteFailureHandler(false, (sinkRecord, failure) -> {
            reported.add(sinkRecord);
            failures.add(failure);
        });

        handler.processFailure(new WriteBatchImpl().withItems(new com.marklogic.client.datamovement.WriteEvent[]{event}),
            new IllegalStateException("write failed"));

        assertEquals(List.of(record), reported);
        assertEquals("write failed", failures.get(0).getMessage());
        assertEquals(AbstractSinkTask.MARKLOGIC_WRITE_FAILURE,
            record.headers().lastWithName(AbstractSinkTask.MARKLOGIC_MESSAGE_FAILURE_HEADER).value());
        assertEquals("/failed/5.json", record.headers().lastWithName(AbstractSinkTask.MARKLOGIC_TARGET_URI).value());
    }

    @Test
    void wrapsNonExceptionThrowableAndIgnoresNonSinkMetadata() {
        SinkRecord record = new SinkRecord("topic", 1, null, null, null, "value", 5L);
        WriteEventImpl sinkRecordEvent = new WriteEventImpl().withTargetUri("/failed/5.json")
            .withMetadata(new SinkRecordMetadataHandle(record));
        WriteEventImpl genericMetadataEvent = new WriteEventImpl()
            .withMetadata(new DocumentMetadataHandle());
        List<Throwable> failures = new ArrayList<>();
        WriteFailureHandler handler = new WriteFailureHandler(true, (sinkRecord, failure) -> failures.add(failure));

        handler.processFailure(new WriteBatchImpl().withItems(new com.marklogic.client.datamovement.WriteEvent[]{
            sinkRecordEvent, genericMetadataEvent
        }), new AssertionError("non-exception failure"));

        assertEquals(1, failures.size());
        assertTrue(failures.get(0) instanceof IOException);
        assertEquals("non-exception failure", failures.get(0).getMessage());
    }

    @Test
    void emptyBatchDoesNotReportFailures() {
        List<SinkRecord> reported = new ArrayList<>();
        WriteFailureHandler handler = new WriteFailureHandler(false,
            (sinkRecord, failure) -> reported.add(sinkRecord));

        handler.processFailure(new WriteBatchImpl().withItems(new com.marklogic.client.datamovement.WriteEvent[0]),
            new IllegalStateException("write failed"));

        assertTrue(reported.isEmpty());
    }
}
