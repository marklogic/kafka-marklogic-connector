package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.junit5.spring.SimpleTestConfig;
import com.marklogic.kafka.connect.AbstractIntegrationTest;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

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
    protected RowBatcherSourceTask startSourceTask(String... configParamNamesAndValues) {
        Map<String, String> config = newMarkLogicConfig(testConfig);
        // Default to a single batch, which is friendly for queries that use limit() and suitable for the small number
        // of rows returned by tests. Tests can still override this by providing their own desired batch size.
        config.put(MarkLogicSourceConfig.DMSDK_BATCH_SIZE, Integer.MAX_VALUE + "");
        config.put(MarkLogicSourceConfig.WAIT_TIME, "0");
        for (int i = 0; i < configParamNamesAndValues.length; i += 2) {
            config.put(configParamNamesAndValues[i], configParamNamesAndValues[i + 1]);
        }
        MarkLogicSourceConnector connector = new MarkLogicSourceConnector();
        connector.start(config);
        RowBatcherSourceTask task;
        try {
            task = (RowBatcherSourceTask) connector.taskClass().getDeclaredConstructor().newInstance();
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
        Assertions.assertEquals(15, sourceRecords.size());
        assertTopicAndSingleValue(sourceRecords, expectedValue);
    }

    private void assertTopicAndSingleValue(List<SourceRecord> newSourceRecords, String expectedValue) {
        AtomicReference<Boolean> foundExpectedValue = new AtomicReference<>(false);
        newSourceRecords.forEach(sourceRecord -> {
            Assertions.assertEquals(AUTHORS_TOPIC, sourceRecord.topic());
            assertTrue(sourceRecord.value() instanceof String, "Until we figure out how to return a JsonNode and make " +
                "Confluent Platform happy, we expect the JsonNode to be toString'ed; type: " + sourceRecord.value().getClass());
            System.out.println(sourceRecord.value());
            if (expectedValue.equals(sourceRecord.value())) {
                foundExpectedValue.set(true);
            }
        });
        Assertions.assertTrue(foundExpectedValue.get(),
            "List of SourceRecords does not contain a record with the expected value");
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
}
