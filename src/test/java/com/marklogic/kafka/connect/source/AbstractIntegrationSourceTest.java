package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.junit5.spring.SimpleTestConfig;
import com.marklogic.kafka.connect.AbstractIntegrationTest;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for any test that wishes to connect to the "kafka-test-test-content" app server on port 8019.
 * AbstractSpringMarkLogicTest assumes it can find mlHost/mlTestRestPort/mlUsername/mlPassword properties in
 * gradle.properties and gradle-local.properties. It uses those to construct a DatabaseClient which can be fetched
 * via getDatabaseClient().
 */
public class AbstractIntegrationSourceTest extends AbstractIntegrationTest {

    // Declared by AbstractSpringMarkLogicTest
    @Autowired
    protected SimpleTestConfig testConfig;

    protected final String AUTHORS_OPTIC_DSL = "op.fromView(\"Medical\", \"Authors\")";
    protected final String AUTHORS_OPTIC_SERIALIZED = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}]}}";
    protected final String AUTHORS_TOPIC = "Authors";


    /**
     * @param configParamNamesAndValues - Configuration values that need to be set for the test.
     * @return a MarkLogicSourceTask based on the default connection config and any optional config params provided by
     * the caller
     */
    protected RowManagerSourceTask startSourceTask(String... configParamNamesAndValues) {
        Map<String, String> config = newMarkLogicConfig(testConfig);
        config.put(MarkLogicSourceConfig.WAIT_TIME, "0");
        for (int i = 0; i < configParamNamesAndValues.length; i += 2) {
            config.put(configParamNamesAndValues[i], configParamNamesAndValues[i + 1]);
        }
        MarkLogicSourceConnector connector = new MarkLogicSourceConnector();
        connector.start(config);
        RowManagerSourceTask task;
        try {
            task = (RowManagerSourceTask) connector.taskClass().getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        task.start(config);
        return task;
    }

    void loadFifteenAuthorsIntoMarkLogic() {
        XMLDocumentManager docMgr = getDatabaseClient().newXMLDocumentManager();
        docMgr.write("citations.xml",
            new DocumentMetadataHandle().withPermission("kafka-test-minimal-user", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE),
            new FileHandle(new File("src/test/ml-data/citations.xml")));
    }

    String loadTestResourceFileIntoString(String filename) throws IOException {
        String templateFilePath = "src/test/resources/" + filename;
        byte[] bytes = Files.readAllBytes(Paths.get(templateFilePath));
        return new String(bytes, StandardCharsets.UTF_8);
    }

    void loadSingleAuthorRowIntoMarkLogicWithCustomTime(String uri, String id, String citationTime, String lastName) throws IOException {
        String template = loadTestResourceFileIntoString("singleAuthorSingleCitation.xml");
        String documentContents = template.replaceAll("%%ID%%", id);
        documentContents = documentContents.replaceAll("%%TIME%%", citationTime);
        documentContents = documentContents.replaceAll("%%LASTNAME%%", lastName);
        XMLDocumentManager docMgr = getDatabaseClient().newXMLDocumentManager();
        docMgr.write(uri,
            new DocumentMetadataHandle().withPermission("kafka-test-minimal-user", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE),
            new StringHandle(documentContents)
        );
    }

    void verifyQueryReturnsFifteenAuthors(List<SourceRecord> sourceRecords, String expectedValue) {
        verifyQueryReturnsFifteenAuthors(sourceRecords, expectedValue, "none");
    }

    void verifyQueryReturnsFifteenAuthors(List<SourceRecord> sourceRecords, String expectedValue, String keyStrategy) {
        assertEquals(15, sourceRecords.size());
        assertTopicAndSingleValue(sourceRecords, expectedValue);
        assertRecordKeys(sourceRecords, keyStrategy);
    }

    private void assertTopicAndSingleValue(List<SourceRecord> newSourceRecords, String expectedValue) {
        AtomicReference<Boolean> foundExpectedValue = new AtomicReference<>(false);
        newSourceRecords.forEach(sourceRecord -> {
            assertEquals(AUTHORS_TOPIC, sourceRecord.topic());
            assertTrue(sourceRecord.value() instanceof String, "Until we figure out how to return a JsonNode and make " +
                "Confluent Platform happy, we expect the JsonNode to be toString'ed; type: " + sourceRecord.value().getClass());
            System.out.println(sourceRecord.value());
            if (expectedValue.equals(sourceRecord.value())) {
                foundExpectedValue.set(true);
            }
        });
        assertTrue(foundExpectedValue.get(),
            "List of SourceRecords does not contain a record with the expected value");
    }

    private void assertRecordKeys(List<SourceRecord> newSourceRecords, String keyStrategy) {
        if(keyStrategy.equalsIgnoreCase("uuid")) {
            newSourceRecords.forEach(sourceRecord -> assertEquals(UUID.fromString(sourceRecord.key().toString()).toString(), sourceRecord.key().toString()));
        } else if(keyStrategy.equalsIgnoreCase("timestamp")) {
            AtomicLong rowNumber = new AtomicLong(1);
            newSourceRecords.forEach(sourceRecord -> {
                String[] keys = sourceRecord.key().toString().split("-");
                assertDoesNotThrow(() -> Long.parseLong(keys[0]));
                assertEquals(rowNumber.getAndIncrement(), Long.parseLong(keys[1]));
            });
        } else {
            newSourceRecords.forEach(sourceRecord -> assertNull(sourceRecord.key()));
        }
    }

    /**
     * Convenience for the common use case of wanting to access the value of each record as a JSON object.
     *
     * @param records - A list of Kafka SourceRecord objects
     * @return - A Stream of Jackson ObjectNodes built from the incoming SourceRecords
     */
    protected Stream<ObjectNode> recordsToJsonObjects(List<SourceRecord> records) {
        return records.stream().map(record -> readJsonObject((String) record.value()));
    }

    protected String appendConstraintOntoQuery(String userDsl, Map<String, Object> parsedConfig, String constraintValue) {
        parsedConfig.put(MarkLogicSourceConfig.DSL_QUERY, userDsl);
        return new DslQueryHandler(null, parsedConfig).appendConstraintAndOrderByToQuery(constraintValue);
    }

    protected void verifyQueryReturnsExpectedRows(String constraintValue, int expectedRowCount, String stringInFirstRecord, Map<String, Object> parsedConfig) {
        parsedConfig.put(MarkLogicSourceConfig.TOPIC, "Authors");
        List<String> params = new ArrayList<>();
        parsedConfig.keySet().forEach(key -> {
            params.add(key);
            params.add(parsedConfig.get(key).toString());
        });

        RowManagerSourceTask task = startSourceTask(params.toArray(new String[]{}));
        task.setConstraintValueStore(new TestConstraintValueStore((String)parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME), constraintValue));
        try {
            List<SourceRecord> newRecords = task.poll();
            assertEquals(expectedRowCount, newRecords.size());
            assertTrue(((String) newRecords.get(0).value()).contains(stringInFirstRecord),
                "Did not find " + stringInFirstRecord + " in " + ((String) newRecords.get(0).value()));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected void loadThreeAuthorDocuments() throws IOException {
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("first", "1", "01:00:00", "First");
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("second", "2", "02:00:00", "Second");
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("Third", "3", "03:00:00", "Third");
    }

    /**
     * Supports tests that want to simulate an existing max value for a constraint column.
     */
    class TestConstraintValueStore extends ConstraintValueStore {

        private String testValue;

        public TestConstraintValueStore(String constraintColumn, String testValue) {
            super(constraintColumn);
            this.testValue = testValue;
        }

        @Override
        public void storeConstraintState(String previousMaxConstraintColumnValue, int lastRowCount) {
        }

        @Override
        public String retrievePreviousMaxConstraintColumnValue() {
            return testValue;
        }
    }
}
