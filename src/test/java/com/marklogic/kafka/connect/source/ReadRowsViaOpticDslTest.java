package com.marklogic.kafka.connect.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReadRowsViaOpticDslTest extends AbstractIntegrationSourceTest {
    protected static final String JSON_RESULT =
        "{\"Medical.Authors.ID\":{\"type\":\"xs:integer\",\"value\":2}," +
            "\"Medical.Authors.LastName\":{\"type\":\"xs:string\",\"value\":\"Pulhoster\"}," +
            "\"Medical.Authors.ForeName\":{\"type\":\"xs:string\",\"value\":\"Misty\"}," +
            "\"Medical.Authors.Date\":{\"type\":\"xs:date\",\"value\":\"2022-05-11\"}," +
            "\"Medical.Authors.DateTime\":{\"type\":\"xs:dateTime\",\"value\":\"2022-05-11T10:00:00\"}}";

    @Test
    void testRowBatcherTask() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

        List<SourceRecord> newSourceRecords = task.poll();
        verifyQueryReturnsFifteenAuthors(newSourceRecords, JSON_RESULT);
    }

    @Test
    void noRowsReturned() throws InterruptedException {
        List<SourceRecord> records = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        ).poll();

        assertNull(records, "When no rows exist, null should be returned; an exception should not be thrown. " +
            "This ensures that bug https://bugtrack.marklogic.com/58240 does not cause an error when a user instead " +
            "expects no data to be returned.");
    }

    @Test
    void fromSql() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        List<SourceRecord> records = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, "op.fromSQL('select LastName, ForeName from medical.authors')",
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        ).poll();

        assertEquals(15, records.size());

        recordsToJsonObjects(records).forEach(row -> {
            assertEquals(2, row.size(), "Expecting 2 columns for LastName and ForeName");
        });
    }

    @Test
    void fromDocUris() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        List<SourceRecord> records = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, "op.fromDocUris(cts.documentQuery('citations.xml')).joinDoc(op.col('doc'), op.col('uri'))",
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        ).poll();

        assertEquals(1, records.size());
        recordsToJsonObjects(records).forEach(row -> {
            assertEquals("xs:string", row.get("uri").get("type").asText());
            assertEquals("citations.xml", row.get("uri").get("value").asText());
            assertEquals("element", row.get("doc").get("type").asText());
            String xmlDoc = row.get("doc").get("value").asText();
            assertTrue(xmlDoc.contains("<Citations>"), "Unexpected doc content: " + xmlDoc);
        });
    }

    @Test
    void noMatchingRows() throws InterruptedException {
        List<SourceRecord> records = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, "op.fromDocUris(cts.documentQuery('no-such-document'))",
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.JSON.toString()
        ).poll();

        assertNull(records, "Should get null back when no rows match; also, check the logging to ensure that " +
            "no exception was thrown");
    }
}
