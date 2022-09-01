package com.marklogic.kafka.connect.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that a DocumentWriteOperation is created correctly based on a SinkRecord.
 */
public class ConvertSinkRecordTest {

    private DefaultSinkRecordConverter converter;

    @Test
    void allPropertiesSet() {
        Map<String, Object> config = new HashMap<>();
        config.put("ml.document.collections", "one,two");
        config.put("ml.document.format", "json");
        config.put("ml.document.mimeType", "application/json");
        config.put("ml.document.permissions", "manage-user,read,manage-admin,update");
        config.put("ml.document.uriPrefix", "/example/");
        config.put("ml.document.uriSuffix", ".json");
        converter = new DefaultSinkRecordConverter(config);

        DocumentWriteOperation op = converter.convert(newSinkRecord("test"));

        assertTrue(op.getUri().startsWith("/example/"));
        assertTrue(op.getUri().endsWith(".json"));


        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        Iterator<String> collections = metadata.getCollections().iterator();
        assertEquals("one", collections.next());
        assertEquals("two", collections.next());

        DocumentMetadataHandle.DocumentPermissions perms = metadata.getPermissions();
        assertEquals(DocumentMetadataHandle.Capability.READ, perms.get("manage-user").iterator().next());
        assertEquals(DocumentMetadataHandle.Capability.UPDATE, perms.get("manage-admin").iterator().next());

        StringHandle content = (StringHandle) op.getContent();
        assertEquals("test", content.get());
        assertEquals(Format.JSON, content.getFormat());
        assertEquals("application/json", content.getMimetype());
    }

    @Test
    void noPropertiesSet() {
        Map<String, Object> kafkaConfig = new HashMap<String, Object>();
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        DocumentWriteOperation op = converter.convert(newSinkRecord("doesn't matter"));

        assertNotNull(op.getUri());
        assertEquals(36, op.getUri().length());

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        assertTrue(metadata.getCollections().isEmpty());
        assertTrue(metadata.getPermissions().isEmpty());
    }

    @Test
    void uriWithUUIDStrategy() {
        Map<String, Object> kafkaConfig = new HashMap<String, Object>();
        kafkaConfig.put("ml.id.strategy", "UUID");
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        DocumentWriteOperation op = converter.convert(newSinkRecord("doesn't matter"));

        assertNotNull(op.getUri());
        assertEquals(36, op.getUri().length());

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        assertTrue(metadata.getCollections().isEmpty());
        assertTrue(metadata.getPermissions().isEmpty());
    }

    @Test
    void uriWithDefaultStrategy() {
        Map<String, Object> kafkaConfig = new HashMap<String, Object>();
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        DocumentWriteOperation op = converter.convert(newSinkRecord("doesn't matter"));

        assertNotNull(op.getUri());
        assertEquals(36, op.getUri().length());

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        assertTrue(metadata.getCollections().isEmpty());
        assertTrue(metadata.getPermissions().isEmpty());
    }

    @Test
    void uriWithKafkaMetaData() {
        Map<String, Object> kafkaConfig = new HashMap<String, Object>();
        kafkaConfig.put("ml.id.strategy", "KAFKA_META_WITH_SLASH");
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        DocumentWriteOperation op = converter.convert(newSinkRecord("doesn't matter"));

        assertEquals("test-topic/1/0", op.getUri());

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        assertTrue(metadata.getCollections().isEmpty());
        assertTrue(metadata.getPermissions().isEmpty());
    }

    @Test
    void uriWithJsonPath() throws IOException {
        JsonNode doc1 = new ObjectMapper().readTree("{\"f1\":\"100\"}");
        Map<String, Object> kafkaConfig = new HashMap<String, Object>();
        kafkaConfig.put("ml.id.strategy", "JSONPATH");
        kafkaConfig.put("ml.id.strategy.paths", "/f1");
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        DocumentWriteOperation op = converter.convert(newSinkRecord(doc1));

        assertNotNull(op.getUri());
        assertEquals("100", op.getUri());

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        assertTrue(metadata.getCollections().isEmpty());
        assertTrue(metadata.getPermissions().isEmpty());
    }

    @Test
    void uriWithHashedJsonPaths() throws IOException {
        JsonNode doc1 = new ObjectMapper().readTree("{\"f1\":\"100\",\"f2\":\"200\"}");
        Map<String, Object> kafkaConfig = new HashMap<String, Object>();
        kafkaConfig.put("ml.id.strategy", "HASH");
        kafkaConfig.put("ml.id.strategy.paths", "/f1,/f2");
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        DocumentWriteOperation op = converter.convert(newSinkRecord(doc1));

        assertNotNull(op.getUri());
        assertEquals(32, op.getUri().length()); //Checking the length. Alternative is to create a MD5 of 100200 and check asserton the values.

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        assertTrue(metadata.getCollections().isEmpty());
        assertTrue(metadata.getPermissions().isEmpty());
    }

    @Test
    void uriWithHashedKafkaMeta() {
        Map<String, Object> kafkaConfig = new HashMap<String, Object>();
        kafkaConfig.put("ml.id.strategy", "KAFKA_META_HASHED");
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        DocumentWriteOperation op = converter.convert(newSinkRecord("doesn't matter"));

        assertNotNull(op.getUri());
        assertEquals(32, op.getUri().length()); //Checking the length. Alternative is to create a MD5 of 100200 and check asserton the values.

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        assertTrue(metadata.getCollections().isEmpty());
        assertTrue(metadata.getPermissions().isEmpty());
    }

    @Test
    void binaryContent() {
        converter = new DefaultSinkRecordConverter(new HashMap<>());

        DocumentWriteOperation op = converter.convert(newSinkRecord("hello world".getBytes()));

        BytesHandle content = (BytesHandle) op.getContent();
        assertEquals("hello world".getBytes().length, content.get().length);
    }

    @Test
    void includeKafkaMetadata() {
        Map<String, Object> kafkaConfig = new HashMap<String, Object>();
        kafkaConfig.put(MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_METADATA, true);
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        final int partition = 5;
        final long offset = 2;
        final String key = "some-key";
        final Long timestamp = System.currentTimeMillis();
        DocumentWriteOperation op = converter.convert(new SinkRecord("topic1", partition, null, key,
            null, "some-value", offset, timestamp, TimestampType.CREATE_TIME));

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        DocumentMetadataHandle.DocumentMetadataValues values = metadata.getMetadataValues();
        assertEquals(key, values.get("kafka-key"));
        assertEquals(offset, Long.parseLong(values.get("kafka-offset")));
        assertEquals(partition, Integer.parseInt(values.get("kafka-partition")));
        assertEquals(timestamp, Long.parseLong(values.get("kafka-timestamp")));
        assertEquals("topic1", values.get("kafka-topic"));
        assertEquals(5, values.keySet().size(), "Only expecting the above 5 keys; bump this expected" +
            " number up in the future if we add metadata that should exist regardless of the setting for including " +
            "Kafka metadata");
    }

    @Test
    void dontIncludeKafkaMetadata() {
        converter = new DefaultSinkRecordConverter(new HashMap<>());

        DocumentWriteOperation op = converter.convert(new SinkRecord("topic1", 5, null, "key1",
            null, "some-value", 123, System.currentTimeMillis(), TimestampType.CREATE_TIME));

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        DocumentMetadataHandle.DocumentMetadataValues values = metadata.getMetadataValues();
        assertNull(values.get("kafka-offset"));
        assertNull(values.get("kafka-key"));
        assertNull(values.get("kafka-partition"));
        assertNull(values.get("kafka-timestamp"));
        assertNull(values.get("kafka-topic"));
    }

    private SinkRecord newSinkRecord(Object value) {
        return new SinkRecord("test-topic", 1, null, null, null, value, 0);
    }
}
