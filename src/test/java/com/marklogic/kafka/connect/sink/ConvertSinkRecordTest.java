package com.marklogic.kafka.connect.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that a DocumentWriteOperation is created correctly based on a SinkRecord.
 */
public class ConvertSinkRecordTest {

    private DefaultSinkRecordConverter converter;
    private final MarkLogicSinkTask markLogicSinkTask = new MarkLogicSinkTask();

    @Test
    public void allPropertiesSet() {
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
    public void noPropertiesSet() {
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
    public void uriWithUUIDStrategy() {
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
    public void uriWithDefaultStrategy() {
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
    public void uriWithKafkaMetaData() {
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
    public void uriWithJsonPath() throws IOException {
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
    public void uriWithHashedJsonPaths() throws IOException {
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
    public void uriWithHashedKafkaMeta() {
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
    public void binaryContent() {
        converter = new DefaultSinkRecordConverter(new HashMap<>());

        DocumentWriteOperation op = converter.convert(newSinkRecord("hello world".getBytes()));

        BytesHandle content = (BytesHandle) op.getContent();
        assertEquals("hello world".getBytes().length, content.get().length);
    }

    @Test
    public void emptyContent() {
        final Collection<SinkRecord> records = new ArrayList<>();
        records.add(newSinkRecord(null));
        markLogicSinkTask.put(records);
    }

    @Test
    public void nullContent() {
        final Collection<SinkRecord> records = new ArrayList<>();
        records.add(null);
        markLogicSinkTask.put(records);
    }

    private SinkRecord newSinkRecord(Object value) {
        return new SinkRecord("test-topic", 1, null, null, null, value, 0);
    }
}
