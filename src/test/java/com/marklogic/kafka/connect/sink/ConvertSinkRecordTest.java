/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.kafka.connect.sink.idstrategy.IdStrategy;
import com.marklogic.kafka.connect.source.DocumentWriteOperationBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a DocumentWriteOperation is created correctly based on a SinkRecord.
 */
class ConvertSinkRecordTest {

    private DefaultSinkRecordConverter converter;

    @Test
    void allPropertiesSet() {
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, "one,two");
        config.put(MarkLogicSinkConfig.DOCUMENT_FORMAT, "json");
        config.put(MarkLogicSinkConfig.DOCUMENT_MIMETYPE, "application/json");
        config.put(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS, "manage-user,read,manage-admin,update");
        config.put(MarkLogicSinkConfig.DOCUMENT_URI_PREFIX, "/example/");
        config.put(MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX, ".json");
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
    void addsTopicToCollectionsWhenConfigured() {
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS_ADD_TOPIC, true);
        config.put(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, "configured");

        DocumentWriteOperation op = new DefaultSinkRecordConverter(config).convert(newSinkRecord("content"));

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        assertTrue(metadata.getCollections().contains("configured"));
        assertTrue(metadata.getCollections().contains("test-topic"));
    }

    @Test
    void noPropertiesSet() {
        Map<String, Object> kafkaConfig = new HashMap<>();
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
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(MarkLogicSinkConfig.ID_STRATEGY, "UUID");
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
        converter = new DefaultSinkRecordConverter(new HashMap<>());

        String firstUri = converter.convert(newSinkRecord("doesn't matter")).getUri();
        String secondUri = converter.convert(newSinkRecord("doesn't matter")).getUri();

        assertDoesNotThrow(() -> UUID.fromString(firstUri));
        assertNotEquals(firstUri, secondUri,
            "The default strategy must generate a new UUID per record so records don't overwrite each other");
    }

    @Test
    void uriWithKafkaMetaData() {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(MarkLogicSinkConfig.ID_STRATEGY, "KAFKA_META_WITH_SLASH");
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        DocumentWriteOperation op = converter.convert(newSinkRecord("doesn't matter"));

        assertEquals("test-topic/1/0", op.getUri());

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        assertTrue(metadata.getCollections().isEmpty());
        assertTrue(metadata.getPermissions().isEmpty());
    }

    @Test
    void jsonPathValidExpression() {
        ObjectMapper m = new ObjectMapper();
        ObjectNode doc = m.createObjectNode();
        doc.putObject("parent").putObject("child").put("hello", "world");

        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSinkConfig.DOCUMENT_URI_PREFIX, "/example/");
        config.put(MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX, ".json");
        config.put(MarkLogicSinkConfig.ID_STRATEGY, "JSONPATH");
        config.put(MarkLogicSinkConfig.ID_STRATEGY_PATH, "/parent/child/hello");
        DocumentWriteOperation op = new DefaultSinkRecordConverter(config).convert(newSinkRecord(doc));

        assertNotNull(op.getUri());
        assertEquals("/example/world.json", op.getUri());

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        assertTrue(metadata.getCollections().isEmpty());
        assertTrue(metadata.getPermissions().isEmpty());
    }

    @Test
    void jsonPathDoesntResolve() {
        ObjectMapper m = new ObjectMapper();
        ObjectNode doc = m.createObjectNode();
        doc.putObject("parent").putObject("child").put("hello", "world");

        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(MarkLogicSinkConfig.ID_STRATEGY, "JSONPATH");
        kafkaConfig.put(MarkLogicSinkConfig.ID_STRATEGY_PATH, "/doesnt/point/to/anything");
        DocumentWriteOperation op = new DefaultSinkRecordConverter(kafkaConfig).convert(newSinkRecord(doc));

        assertNotNull(op.getUri());
        assertEquals("", op.getUri(), "This is the current behavior as of 1.8.0 but it does not seem correct; seems " +
            "like a UUID should be returned instead so that duplicate URIs are not generated. DEVEXP-541 was " +
            "opened to fix this.");
    }

    @Test
    void jsonPathUsesUuidWhenContentIsInvalidJson() {
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSinkConfig.ID_STRATEGY, "JSONPATH");
        config.put(MarkLogicSinkConfig.ID_STRATEGY_PATH, "/id");

        DocumentWriteOperation op = new DefaultSinkRecordConverter(config).convert(newSinkRecord("{invalid"));

        assertTrue(op.getUri().matches("[0-9a-fA-F-]{36}"));
    }

    @Test
    void hashedJsonPathsUsesUuidWhenContentIsInvalidJson() {
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSinkConfig.ID_STRATEGY, "HASH");
        config.put(MarkLogicSinkConfig.ID_STRATEGY_PATH, "/id");

        DocumentWriteOperation op = new DefaultSinkRecordConverter(config).convert(newSinkRecord("{invalid"));

        assertTrue(op.getUri().matches("[0-9a-fA-F-]{36}"));
    }

    @Test
    void uriWithHashedJsonPaths() throws IOException {
        JsonNode doc1 = new ObjectMapper().readTree("{\"f1\":\"100\",\"f2\":\"200\"}");
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(MarkLogicSinkConfig.ID_STRATEGY, "HASH");
        kafkaConfig.put(MarkLogicSinkConfig.ID_STRATEGY_PATH, "/f1,/f2");
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        DocumentWriteOperation op = converter.convert(newSinkRecord(doc1));

        assertNotNull(op.getUri());
        assertEquals("3702198ae154d22d8e390f21a73f53a0446ae067410a3a227d0ad2ab9e4a5144a6d3a1909c5badfbe1277a78840b63b567c43977971e35198bf068054f0b8816",
            op.getUri(), "Expected SHA-512 of the selected values in configured path order");

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        assertTrue(metadata.getCollections().isEmpty());
        assertTrue(metadata.getPermissions().isEmpty());
    }

    @Test
    void uriWithHashedKafkaMeta() {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(MarkLogicSinkConfig.ID_STRATEGY, "KAFKA_META_HASHED");
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        DocumentWriteOperation op = converter.convert(newSinkRecord("doesn't matter"));

        assertNotNull(op.getUri());
        assertEquals("c343ed3c34e6c2c9dbc9f7e3e60416fbb026bf965bdd60e08c08af7127ba7126a35a758d7afaac85b768f5ed01798db657fc69d172e95474f82e980238f1bd41",
            op.getUri(), "Expected SHA-512 of topic, partition, and offset");

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
    void binaryContentUsesConfiguredFormatAndMimeType() {
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSinkConfig.DOCUMENT_FORMAT, "binary");
        config.put(MarkLogicSinkConfig.DOCUMENT_MIMETYPE, "application/octet-stream");

        DocumentWriteOperation op = new DefaultSinkRecordConverter(config).convert(newSinkRecord(new byte[]{1, 2, 3}));

        BytesHandle content = (BytesHandle) op.getContent();
        assertEquals(Format.BINARY, content.getFormat());
        assertEquals("application/octet-stream", content.getMimetype());
        assertEquals(3, content.get().length);
    }

    @Test
    void mapContentIsSerializedAsJson() {
        Map<String, Object> value = new HashMap<>();
        value.put("name", "Kafka");

        DocumentWriteOperation op = new DefaultSinkRecordConverter(new HashMap<>()).convert(newSinkRecord(value));

        BytesHandle content = (BytesHandle) op.getContent();
        assertEquals("{\"name\":\"Kafka\"}", new String(content.get()));
    }

    @Test
    void structContentIsSerializedAsJson() {
        Schema schema = SchemaBuilder.struct().name("test.Record")
            .field("name", Schema.STRING_SCHEMA)
            .build();
        Struct value = new Struct(schema).put("name", "Kafka");
        SinkRecord record = new SinkRecord("topic1", 0, null, null, schema, value, 0L);

        DocumentWriteOperation op = new DefaultSinkRecordConverter(new HashMap<>()).convert(record);

        BytesHandle content = (BytesHandle) op.getContent();
        assertEquals("{\"name\":\"Kafka\"}", new String(content.get()));
    }

    @Test
    void schemaBackedNonStructContentIsPreservedAsString() {
        SinkRecord record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, "plain text", 0L);

        DocumentWriteOperation op = new DefaultSinkRecordConverter(new HashMap<>()).convert(record);

        assertEquals("plain text", ((StringHandle) op.getContent()).get());
    }

    @Test
    void nullRecordAndNullValueAreRejected() {
        converter = new DefaultSinkRecordConverter(new HashMap<>());

        assertEquals("Sink record must not be null and must have a value",
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> converter.convert(null)).getMessage());
        assertEquals("Sink record must not be null and must have a value",
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> converter.convert(newSinkRecord(null))).getMessage());
    }

    @Test
    void includeKafkaMetadata() {
        Map<String, Object> kafkaConfig = new HashMap<>();
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
    void includeKafkaMetadataWhenOptionalFieldsAreNull() {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_METADATA, true);
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        SinkRecord record = new SinkRecord("topic1", 1, null, null, null, "some-value", 2L, null,
            TimestampType.NO_TIMESTAMP_TYPE);
        DocumentWriteOperation op = converter.convert(record);

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        DocumentMetadataHandle.DocumentMetadataValues values = metadata.getMetadataValues();
        assertEquals("topic1", values.get("kafka-topic"));
        assertEquals("2", values.get("kafka-offset"));
        assertNull(values.get("kafka-key"));
        assertEquals("1", values.get("kafka-partition"));
        assertNull(values.get("kafka-timestamp"));
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

    @Test
    void includeKafkaHeadersWithPrefix() {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_HEADERS, true);
        kafkaConfig.put(MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_HEADERS_PREFIX, "kafkaHeader_");
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        final int partition = 5;
        final long offset = 2;
        final String key = "some-key";
        final Long timestamp = System.currentTimeMillis();
        List<Header> headers = new ArrayList<Header>() {{
            add(new TestHeaders("A", "1"));
            add(new TestHeaders("B", "2"));
        }};
        DocumentWriteOperation op = converter.convert(new SinkRecord("topic1", partition, null, key,
            null, "some-value", offset, timestamp, TimestampType.CREATE_TIME, headers));

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        DocumentMetadataHandle.DocumentMetadataValues values = metadata.getMetadataValues();
        assertEquals("1", values.get("kafkaHeader_A"));
        assertEquals("2", values.get("kafkaHeader_B"));
        assertNull(values.get("kafka-offset"));
    }

    @Test
    void includeKafkaHeadersWithoutPrefix() {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_HEADERS, true);
        converter = new DefaultSinkRecordConverter(kafkaConfig);

        final int partition = 5;
        final long offset = 2;
        final String key = "some-key";
        final Long timestamp = System.currentTimeMillis();
        List<Header> headers = new ArrayList<Header>() {{
            add(new TestHeaders("A", "1"));
            add(new TestHeaders("B", "2"));
        }};
        DocumentWriteOperation op = converter.convert(new SinkRecord("topic1", partition, null, key,
            null, "some-value", offset, timestamp, TimestampType.CREATE_TIME, headers));

        DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
        DocumentMetadataHandle.DocumentMetadataValues values = metadata.getMetadataValues();
        assertEquals("1", values.get("A"));
        assertEquals("2", values.get("B"));
        assertNull(values.get("kafka-offset"));
    }

    static class TestHeaders implements Header {
        private final String key;
        private final String value;
        TestHeaders(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Schema schema() {
            return null;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public Header with(Schema schema, Object o) {
            return null;
        }

        @Override
        public Header rename(String s) {
            return null;
        }
    }

    @Test
    void usesAnInjectedDocumentWriteOperationBuilder() {
        converter = new DefaultSinkRecordConverter(new HashMap<>());
        DocumentWriteOperationBuilder builder = new DocumentWriteOperationBuilder()
            .withUriPrefix("/injected/")
            .withUriSuffix(".json");
        converter.setDocumentWriteOperationBuilder(builder);

        DocumentWriteOperation op = converter.convert(newSinkRecord("content"));

        assertSame(builder, converter.getDocumentWriteOperationBuilder());
        assertTrue(op.getUri().startsWith("/injected/"));
        assertTrue(op.getUri().endsWith(".json"));
    }

    private SinkRecord newSinkRecord(Object value) {
        return new SinkRecord("test-topic", 1, null, null, null, value, 0);
    }

    @Test
    void defaultIdStrategyGeneratesUuid() {
        String generated = new IdStrategy() {}.generateId(null, "topic", 0, 0L);
        assertTrue(generated.matches("[0-9a-fA-F-]{36}"));
    }
}
