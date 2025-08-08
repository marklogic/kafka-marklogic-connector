/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for any test that wishes to connect to the
 * "kafka-test-test-content" app server on port 8019.
 * AbstractSpringMarkLogicTest assumes it can find
 * mlHost/mlTestRestPort/mlUsername/mlPassword properties in
 * gradle.properties and gradle-local.properties. It uses those to construct a
 * DatabaseClient which can be fetched
 * via getDatabaseClient().
 */
public abstract class AbstractIntegrationSourceTest extends AbstractIntegrationTest {

    // Declared by AbstractSpringMarkLogicTest
    @Autowired
    protected SimpleTestConfig testConfig;

    protected final String AUTHORS_OPTIC_DSL = "op.fromView(\"Medical\", \"Authors\")";
    protected final String AUTHORS_ORDERED_BY_ID_OPTIC_DSL = AUTHORS_OPTIC_DSL + ".orderBy(op.asc(op.col('ID')))";
    protected final String AUTHORS_OPTIC_SERIALIZED = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}]}}";
    protected final String AUTHORS_TOPIC = "Authors";

    /**
     * @param configParamNamesAndValues - Configuration values that need to be set
     *                                  for the test.
     * @return a MarkLogicSourceTask based on the default connection config and any
     *         optional config params provided by
     *         the caller
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
                new DocumentMetadataHandle().withPermission("kafka-test-minimal-user",
                        DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE),
                new FileHandle(new File("src/test/ml-data/citations.xml")));
    }

    String loadTestResourceFileIntoString(String filename) throws IOException {
        String templateFilePath = "src/test/resources/" + filename;
        byte[] bytes = Files.readAllBytes(Paths.get(templateFilePath));
        return new String(bytes, StandardCharsets.UTF_8);
    }

    void loadSingleAuthorRowIntoMarkLogicWithCustomTime(String uri, String id, String citationTime, String lastName)
            throws IOException {
        String template = loadTestResourceFileIntoString("singleAuthorSingleCitation.xml");
        String documentContents = template.replaceAll("%%ID%%", id);
        documentContents = documentContents.replaceAll("%%TIME%%", citationTime);
        documentContents = documentContents.replaceAll("%%LASTNAME%%", lastName);
        XMLDocumentManager docMgr = getDatabaseClient().newXMLDocumentManager();
        docMgr.write(uri,
                new DocumentMetadataHandle().withPermission("kafka-test-minimal-user",
                        DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE),
                new StringHandle(documentContents));
    }

    void verifyQueryReturnsFifteenAuthors(List<SourceRecord> sourceRecords, String expectedValue) {
        assertEquals(15, sourceRecords.size());
        assertTopicAndSingleValue(sourceRecords, expectedValue);
    }

    private void assertTopicAndSingleValue(List<SourceRecord> newSourceRecords, String expectedValue) {
        AtomicReference<Boolean> foundExpectedValue = new AtomicReference<>(false);
        newSourceRecords.forEach(sourceRecord -> {
            assertEquals(AUTHORS_TOPIC, sourceRecord.topic());
            assertTrue(sourceRecord.value() instanceof String,
                    "Until we figure out how to return a JsonNode and make " +
                            "Confluent Platform happy, we expect the JsonNode to be toString'ed; type: "
                            + sourceRecord.value().getClass());
            if (expectedValue.equals(sourceRecord.value())) {
                foundExpectedValue.set(true);
            }
        });
        assertTrue(foundExpectedValue.get(),
                "List of SourceRecords does not contain a record with the expected value");
    }

    /**
     * Convenience for the common use case of wanting to access the value of each
     * record as a JSON object.
     *
     * @param records - A list of Kafka SourceRecord objects
     * @return - A Stream of Jackson ObjectNodes built from the incoming
     *         SourceRecords
     */
    protected Stream<ObjectNode> recordsToJsonObjects(List<SourceRecord> records) {
        return records.stream().map(record -> readJsonObject((String) record.value()));
    }

    protected String appendConstraintOntoQuery(String userDsl, Map<String, Object> parsedConfig,
            String constraintValue) {
        parsedConfig.put(MarkLogicSourceConfig.DSL_QUERY, userDsl);
        return new DslQueryHandler(null, parsedConfig).appendConstraintAndOrderByToQuery(constraintValue);
    }

    protected void verifyQueryReturnsExpectedRows(String constraintValue, int expectedRowCount,
            String stringInFirstRecord, Map<String, Object> parsedConfig) {
        parsedConfig.put(MarkLogicSourceConfig.TOPIC, "Authors");
        List<String> params = new ArrayList<>();
        parsedConfig.keySet().forEach(key -> {
            params.add(key);
            params.add(parsedConfig.get(key).toString());
        });

        RowManagerSourceTask task = startSourceTask(params.toArray(new String[] {}));
        task.setConstraintValueStore(new TestConstraintValueStore(
                (String) parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME), constraintValue));
        try {
            List<SourceRecord> newRecords = task.poll();
            assertNotNull(newRecords,
                    "poll() unexpectedly returned null; params: " + params + "; constraintValue: " + constraintValue);
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

    protected void verifyRecordKeysAreSetToIDColumn(List<SourceRecord> records) {
        assertEquals("1", records.get(0).key(), "The key should be populated by the ID column, and the records are " +
                "expected to be ordered by ID ascending, so the first record should have a key of 1");
        assertEquals("5", records.get(14).key(), "The records are expected to be ordered by ID ascending, so the " +
                "last record should have a key of 5");
    }

    /**
     * Supports tests that want to simulate an existing max value for a constraint
     * column.
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
