package com.marklogic.kafka.connect.sink;

import com.marklogic.client.datamovement.WriteBatch;
import com.marklogic.client.io.DocumentMetadataHandle;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class HandleWriteFailureTest extends AbstractIntegrationTest {

    /**
     * This test is expected to log the URI of each failed document. We don't yet have a way to capture logging output
     * to inspect it, so that part has to be done manually. The test does verify that the failed document contains
     * Kafka metadata so that it can be logged.
     */
    @Test
    void handleWriteFailure() {
        final String TEST_COLLECTION = "handle-write-failure-test";

        MarkLogicSinkTask task = startSinkTask(
            MarkLogicSinkConfig.DOCUMENT_FORMAT, "json",
            MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, TEST_COLLECTION,
            MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_METADATA, "true"
        );

        // Register our own failure listener to inspect the failed batch
        List<WriteBatch> failedBatches = new ArrayList<>();
        task.getWriteBatcher().onBatchFailure(((batch, failure) -> {
            failedBatches.add(batch);
        }));

        final String topic = "topic1";
        final int partition = 1;
        final String key = "key123";
        final String value = "<not>json</not>";
        final long offset = 123;
        final Long timestamp = System.currentTimeMillis();

        SinkRecord record = new SinkRecord(topic, partition, null, key, null, value, offset,
            timestamp, TimestampType.CREATE_TIME);
        putSinkRecord(task, record);

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
    }

    /**
     * This is just intended to ensure that null values in the Kafka metadata do not cause an error.
     */
    @Test
    void handleWriteFailureWithMinimalKafkaMetadata() {
        MarkLogicSinkTask task = startSinkTask(
            MarkLogicSinkConfig.DOCUMENT_FORMAT, "json",
            MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_METADATA, "true"
        );

        List<WriteBatch> failedBatches = new ArrayList<>();
        task.getWriteBatcher().onBatchFailure(((batch, failure) -> {
            failedBatches.add(batch);
        }));

        final String topic = "topic1";
        final int partition = 1;
        final String value = "<not>json</not>";
        final long offset = 123;
        // Based on the constructor, it seems that partition/offset are guaranteed to exist, and we know topic will
        // exist as well.
        SinkRecord record = new SinkRecord(topic, partition, null, null, null, value, offset);
        putSinkRecord(task, record);

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
