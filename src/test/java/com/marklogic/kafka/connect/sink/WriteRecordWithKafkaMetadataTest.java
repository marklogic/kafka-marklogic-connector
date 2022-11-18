package com.marklogic.kafka.connect.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.DocumentMetadataHandle;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WriteRecordWithKafkaMetadataTest extends AbstractIntegrationSinkTest {

    /**
     * Verifies that when the option is set for including Kafka metadata, that metadata is present in the document
     * metadata of each written document. The primary use case for this is to help with error logging by ensuring that
     * the Kafka metadata is available to a DMSDK failure listener. Users may benefit too from the document metadata
     * being available for inspection/query purposes.
     */
    @Test
    void includeKafkaMetadata() {
        final String TEST_COLLECTION = "include-kafka-metadata-test";

        AbstractSinkTask task = startSinkTask(
            MarkLogicSinkConfig.DOCUMENT_FORMAT, "json",
            MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, TEST_COLLECTION,
            MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_METADATA, "true",
            MarkLogicSinkConfig.ID_STRATEGY, "JSONPATH",
            MarkLogicSinkConfig.ID_STRATEGY_PATH, "/id",
            MarkLogicSinkConfig.DOCUMENT_URI_PREFIX, "/TEST/",
            MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX, ".json"
        );

        ObjectNode content = new ObjectMapper().createObjectNode();
        content.put("id", 100);
        content.put("hello", "world");

        final String topic = "topic1";
        final int partition = 1;
        final String key = "key123";
        final long offset = 123;
        final Long timestamp = System.currentTimeMillis();

        SinkRecord record = new SinkRecord(topic, partition, null, key, null, content.toString(),
            offset, timestamp, TimestampType.CREATE_TIME);
        putAndFlushRecords(task, record);

        assertCollectionSize(TEST_COLLECTION, 1);

        DocumentMetadataHandle metadata = getDatabaseClient().newJSONDocumentManager()
            .readMetadata("/TEST/100.json", new DocumentMetadataHandle());
        DocumentMetadataHandle.DocumentMetadataValues values = metadata.getMetadataValues();
        assertEquals(topic, values.get("kafka-topic"));
        assertEquals(partition, Integer.parseInt(values.get("kafka-partition")));
        assertEquals(key, values.get("kafka-key"));
        assertEquals(offset, Long.parseLong(values.get("kafka-offset")));
        assertEquals(timestamp, Long.parseLong(values.get("kafka-timestamp")));
    }
}
