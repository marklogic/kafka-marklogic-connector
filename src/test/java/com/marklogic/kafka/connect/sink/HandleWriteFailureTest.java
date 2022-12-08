package com.marklogic.kafka.connect.sink;

import com.marklogic.client.datamovement.WriteBatch;
import com.marklogic.client.io.DocumentMetadataHandle;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.test.util.AssertionErrors.assertNotNull;

public class HandleWriteFailureTest extends AbstractIntegrationSinkTest {

    final private List<ErrRecord> reportedSinkRecords = new ArrayList<>();
    /**
     * This test is expected to log the URI of each failed document. We don't yet have a way to capture logging output
     * to inspect it, so that part has to be done manually. The test does verify that the failed document contains
     * Kafka metadata so that it can be logged.
     */
    @Test
    void handleWriteFailure() {
        final String TEST_COLLECTION = "handle-write-failure-test";

        WriteBatcherSinkTask task = (WriteBatcherSinkTask) startSinkTask(
            (SinkRecord record, Throwable e) -> reportedSinkRecords.add(new ErrRecord(record, e)),
            MarkLogicSinkConfig.DOCUMENT_FORMAT, "json",
            MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, TEST_COLLECTION,
            MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_METADATA, "true"
        );

        // Register our own failure listener to inspect the failed batch
        List<WriteBatch> failedBatches = new ArrayList<>();
        task.getWriteBatcher().onBatchFailure(((batch, failure) -> failedBatches.add(batch)));

        final String topic = "topic1";
        final int partition = 1;
        final String key = "key123";
        final String badValue = "<not>json</not>";
        final String goodValue = "{\"A\": \"a\"}";
        final long offset = 123;
        final Long timestamp = System.currentTimeMillis();

        SinkRecord badRecord = new SinkRecord(topic, partition, null, key, null, badValue, offset,
            timestamp, TimestampType.CREATE_TIME);
        SinkRecord goodRecord = new SinkRecord(topic, partition, null, key, null, goodValue, offset,
            timestamp, TimestampType.CREATE_TIME);
        putAndFlushRecords(task, badRecord, goodRecord);

        assertCollectionSize("The target collection should be empty since the content was not valid JSON",
            TEST_COLLECTION, 0);

        assertEquals(1, failedBatches.size());
        DocumentMetadataHandle metadata = (DocumentMetadataHandle) failedBatches.get(0).getItems()[0].getMetadata();
        DocumentMetadataHandle.DocumentMetadataValues values = metadata.getMetadataValues();
        assertEquals(topic, values.get("kafka-topic"));
        assertEquals(partition, Integer.parseInt(values.get("kafka-partition")));
        assertEquals(key, values.get("kafka-key"));
        assertEquals(offset, Long.parseLong(values.get("kafka-offset")));
        assertEquals(timestamp, Long.parseLong(values.get("kafka-timestamp")));

        Assertions.assertEquals(2, reportedSinkRecords.size(),
            "The mock error reporter should have been called with 2 SinkRecords");
        reportedSinkRecords.forEach((reportedSinkRecord) -> {
            Assertions.assertEquals(reportedSinkRecord.reportedException.getMessage(),
                "Local message: failed to apply resource at documents: Bad Request. Server Message: XDMP-JSONDOC: xdmp:get-request-part-body(\"json\") -- Document is not JSON",
                "The reported exception message does not match the expected message");
            Headers headers = reportedSinkRecord.sinkRecord.headers();
            assertEquals(4, headers.size());
            assertEquals(AbstractSinkTask.MARKLOGIC_WRITE_FAILURE, headers.lastWithName(AbstractSinkTask.MARKLOGIC_MESSAGE_FAILURE_HEADER).value());
            assertEquals("Local message: failed to apply resource at documents: Bad Request. Server Message: XDMP-JSONDOC: xdmp:get-request-part-body(\"json\") -- Document is not JSON",
                headers.lastWithName(AbstractSinkTask.MARKLOGIC_MESSAGE_EXCEPTION_MESSAGE).value());
            assertEquals(topic, headers.lastWithName(AbstractSinkTask.MARKLOGIC_ORIGINAL_TOPIC).value());
            assertNotNull("The SinkRecord should have a target URI set", headers.lastWithName(AbstractSinkTask.MARKLOGIC_TARGET_URI).value());
        });
    }

    /**
     * This is just intended to ensure that null values in the Kafka metadata do not cause an error.
     */
    @Test
    void handleWriteFailureWithMinimalKafkaMetadata() {
        WriteBatcherSinkTask task = (WriteBatcherSinkTask) startSinkTask(
            MarkLogicSinkConfig.DOCUMENT_FORMAT, "json",
            MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_METADATA, "true"
        );

        List<WriteBatch> failedBatches = new ArrayList<>();
        task.getWriteBatcher().onBatchFailure(((batch, failure) -> failedBatches.add(batch)));

        final String topic = "topic1";
        final int partition = 1;
        final String value = "<not>json</not>";
        final long offset = 123;
        // Based on the constructor, it seems that partition/offset are guaranteed to exist, and we know topic will
        // exist as well.
        SinkRecord record = new SinkRecord(topic, partition, null, null, null, value, offset);
        putAndFlushRecords(task, record);

        assertEquals(1, failedBatches.size());
        DocumentMetadataHandle metadata = (DocumentMetadataHandle) failedBatches.get(0).getItems()[0].getMetadata();
        DocumentMetadataHandle.DocumentMetadataValues values = metadata.getMetadataValues();
        assertEquals(topic, values.get("kafka-topic"));
        assertEquals(partition, Integer.parseInt(values.get("kafka-partition")));
        assertNull(values.get("kafka-key"));
        assertEquals(offset, Long.parseLong(values.get("kafka-offset")));
        assertNull(values.get("kafka-timestamp"));
    }
}

class ErrRecord {
    Throwable reportedException;
    SinkRecord sinkRecord;
    ErrRecord(SinkRecord sinkRecord, Throwable throwable) {
        this.sinkRecord = sinkRecord;
        this.reportedException = throwable;
    }
}
