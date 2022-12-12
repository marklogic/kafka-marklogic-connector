package com.marklogic.kafka.connect.source;

import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.FileHandle;
import com.marklogic.junit5.spring.SimpleTestConfig;
import com.marklogic.kafka.connect.AbstractIntegrationTest;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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
            new FileHandle(new File("src/test/resources/citations.xml")));
    }

    void verifyQueryReturnsFifteenAuthors(List<SourceRecord> sourceRecords) {
        Assertions.assertEquals(15, sourceRecords.size());
        assertTopicAndSingleValue(sourceRecords, AUTHORS_TOPIC);
    }

    private void assertTopicAndSingleValue(List<SourceRecord> newSourceRecords, String topic) {
        String expectedValue = "{\"Medical.Authors.ID\":{\"type\":\"xs:integer\",\"value\":2}," +
            "\"Medical.Authors.LastName\":{\"type\":\"xs:string\",\"value\":\"Pulhoster\"}," +
            "\"Medical.Authors.ForeName\":{\"type\":\"xs:string\",\"value\":\"Misty\"}}";
        AtomicReference<Boolean> foundExpectedValue = new AtomicReference<>(false);
        newSourceRecords.forEach(sourceRecord -> {
            Assertions.assertEquals(topic, sourceRecord.topic());
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
}
