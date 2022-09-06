package com.marklogic.kafka.connect.sink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class WriteViaBulkDataServicesTest extends AbstractIntegrationTest {

    private final static String TEST_COLLECTION = "bulk-ds-test";
    private final static String TEST_BULK_API_URI = "/example/bulk-endpoint.api";
    private final static String MODULES_DATABASE = "kafka-test-modules";

    @Test
    void twoBatches() {
        AbstractSinkTask task = startSinkTask(
            MarkLogicSinkConfig.BULK_DS_API_URI, TEST_BULK_API_URI,
            MarkLogicSinkConfig.CONNECTION_MODULES_DATABASE, MODULES_DATABASE,
            // Turning on logging here to get coverage of this code; not possibly to verify the output via assertions
            MarkLogicSinkConfig.LOGGING_RECORD_HEADERS, "true",
            MarkLogicSinkConfig.LOGGING_RECORD_KEY, "true"
        );

        task.put(Arrays.asList(newSinkRecord("<first/>"), newSinkRecord("<second/>")));

        assertCollectionSize("No records should exist yet since the batch size on the API declaration is 3, and " +
            "only 2 records have been written so far", TEST_COLLECTION, 0);

        task.put(Arrays.asList(newSinkRecord("<third/>"), newSinkRecord("<fourth/>")));

        assertCollectionSize("Because the batch size in the API declaration is 3, the addition of two more records " +
            "should force the first batch of 3 to be written to ML, and then leave the 4th record sitting in the " +
            "BulkInputCaller queue", TEST_COLLECTION, 3);

        task.flush(null);

        assertCollectionSize("All 4 records should have been written, as the flush call will TBD", TEST_COLLECTION, 4);
    }

    @Test
    void verifyEndpointConstants() {
        AbstractSinkTask task = startSinkTask(
            MarkLogicSinkConfig.BULK_DS_API_URI, TEST_BULK_API_URI,
            MarkLogicSinkConfig.CONNECTION_MODULES_DATABASE, MODULES_DATABASE,
            MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, "json-test",
            MarkLogicSinkConfig.DOCUMENT_URI_PREFIX, "/json/",
            MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX, ".json",
            MarkLogicSinkConfig.DOCUMENT_FORMAT, "json",
            MarkLogicSinkConfig.DOCUMENT_MIMETYPE, "application/json",
            MarkLogicSinkConfig.DOCUMENT_COLLECTIONS_ADD_TOPIC, "true",
            MarkLogicSinkConfig.DOCUMENT_PERMISSIONS, "rest-reader,read,rest-writer,update",
            MarkLogicSinkConfig.DOCUMENT_TEMPORAL_COLLECTION, "some-temporal-collection"
        );

        final String content = "{\"hello\":\"world\"}";
        putAndFlushRecords(task, newSinkRecord(content));

        assertCollectionSize(TEST_COLLECTION, 1);
        JsonNode doc = readJsonDocument(getUrisInCollection(TEST_COLLECTION, 1).get(0));
        assertEquals(content, doc.get("content").asText(), "The test endpoint just tosses whatever 'input' it receives " +
            "into a 'content' field; it doesn't bother trying to convert it into real JSON. An endpoint developer is " +
            "free to do whatever they'd like with the content.");

        JsonNode constants = doc.get("endpointConstants");
        assertNotNull(constants, "The test endpoint - bulk-endpoint.sjs - is expected to stuff the endpoint constants " +
            "passed to it so that a test like this can verify that the appropriate constants are sent. The endpoint " +
            "doesn't have to do anything with these. They're provided in case an endpoint developer wants to make " +
            "their endpoint a bit more dynamic by having it driven via ml.document.* properties provided to the " +
            "connector.");

        assertEquals("json-test", constants.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS).asText());
        assertEquals("/json/", constants.get(MarkLogicSinkConfig.DOCUMENT_URI_PREFIX).asText());
        assertEquals(".json", constants.get(MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX).asText());
        assertEquals("json", constants.get(MarkLogicSinkConfig.DOCUMENT_FORMAT).asText());
        assertEquals("application/json", constants.get(MarkLogicSinkConfig.DOCUMENT_MIMETYPE).asText());
        assertEquals("true", constants.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS_ADD_TOPIC).asText());
        assertEquals("rest-reader,read,rest-writer,update", constants.get(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS).asText());
        assertEquals("some-temporal-collection", constants.get(MarkLogicSinkConfig.DOCUMENT_TEMPORAL_COLLECTION).asText());

        assertEquals(8, constants.size(), "Expecting the 8 ml.document.* fields; if new features add more fields to " +
            "this, that's fine; just update this assertion to match the exact number of expected fields");
    }

    /**
     * This could be combined with the test for endpoint constants, as they both have the same scenario of ingesting
     * a single document. But they're kept separate to make the assertions easier to manage and understand.
     */
    @Test
    void verifyKafkaMetadata() {
        AbstractSinkTask task = startSinkTask(
            MarkLogicSinkConfig.BULK_DS_API_URI, TEST_BULK_API_URI,
            MarkLogicSinkConfig.CONNECTION_MODULES_DATABASE, MODULES_DATABASE
        );

        final String topic = "topic1";
        final int partition = 1;
        final String key = "key123";
        final String content = "<test/>";
        final long offset = 123;
        final Long timestamp = System.currentTimeMillis();
        SinkRecord record = new SinkRecord(topic, partition, null, key, null, content, offset,
            timestamp, TimestampType.CREATE_TIME);

        putAndFlushRecords(task, record);

        String uri = getUrisInCollection(TEST_COLLECTION, 1).get(0);
        JsonNode doc = readJsonDocument(uri);
        assertEquals(content, doc.get("content").asText());

        JsonNode metadata = doc.get("kafkaMetadata");
        assertEquals(topic, metadata.get("topic").asText());
        assertEquals(key, metadata.get("key").asText());
        assertEquals(offset, metadata.get("offset").asLong());
        assertEquals(partition, metadata.get("partition").asInt());
        assertEquals(timestamp, metadata.get("timestamp").asLong());
    }

    @Test
    void missingModulesDatabase() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> startSinkTask(
            MarkLogicSinkConfig.BULK_DS_API_URI, TEST_BULK_API_URI
        ));
        assertTrue(ex.getMessage().contains("Cannot read Bulk Data Services API declaration"),
            "Unexpected error: " + ex.getMessage());
    }

    @Test
    void invalidApiUri() {
        RuntimeException ex = assertThrows(RuntimeException.class, () -> startSinkTask(
            MarkLogicSinkConfig.BULK_DS_API_URI, "/this-doesnt-exist.api",
            MarkLogicSinkConfig.CONNECTION_MODULES_DATABASE, MODULES_DATABASE
        ));
        String message = ex.getMessage();
        assertTrue(message.startsWith("Unable to read Bulk Data Services API declaration at URI: /this-doesnt-exist.api"),
            "Unexpected message: " + message);
        assertTrue(message.contains("Could not read non-existent document"), "Unexpected message: " + message);
    }

    @Test
    void badBulkEndpoint() {
        AbstractSinkTask task = startSinkTask(
            MarkLogicSinkConfig.BULK_DS_API_URI, "/example/bad-bulk-endpoint.api",
            MarkLogicSinkConfig.CONNECTION_MODULES_DATABASE, MODULES_DATABASE
        );

        putAndFlushRecords(task, newSinkRecord("content-doesnt-matter"));

        // The error handler is expected to log the error. That can't yet be verified automatically, so run this and
        // inspect the logging manually.
        assertCollectionSize("The write is expected to have failed, so the collection should be empty", TEST_COLLECTION, 0);
    }

}
