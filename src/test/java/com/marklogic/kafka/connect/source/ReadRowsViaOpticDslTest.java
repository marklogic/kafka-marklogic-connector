package com.marklogic.kafka.connect.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadRowsViaOpticDslTest extends AbstractIntegrationSourceTest {
    protected static final String JSON_RESULT =
        "{\"Medical.Authors.ID\":{\"type\":\"xs:integer\",\"value\":2}," +
            "\"Medical.Authors.LastName\":{\"type\":\"xs:string\",\"value\":\"Pulhoster\"}," +
            "\"Medical.Authors.ForeName\":{\"type\":\"xs:string\",\"value\":\"Misty\"}," +
            "\"Medical.Authors.Date\":{\"type\":\"xs:date\",\"value\":\"2022-05-11\"}," +
            "\"Medical.Authors.DateTime\":{\"type\":\"xs:dateTime\",\"value\":\"2022-05-11T10:00:00\"}}";

    @Test
    void readFifteenAuthorsAsJson() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        RowManagerSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL + ".orderBy(op.asc(op.col('ID')))",
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.ROW_LIMIT, "1000",
            MarkLogicSourceConfig.KEY_COLUMN, "Medical.Authors.ID"
        );

        List<SourceRecord> newSourceRecords = task.poll();
        verifyQueryReturnsFifteenAuthors(newSourceRecords, JSON_RESULT);

        assertEquals("1", newSourceRecords.get(0).key(), "The key should be populated by the ID column");
        assertEquals("5", newSourceRecords.get(14).key());
    }

    @Test
    void keyColumnDoesntExist() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        RowManagerSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.KEY_COLUMN, "column-doesnt-exist"
        );

        List<SourceRecord> sourceRecords = task.poll();
        verifyQueryReturnsFifteenAuthors(sourceRecords, JSON_RESULT);

        sourceRecords.forEach(sourceRecord -> {
            assertNull(sourceRecord.key(), "If the column is not found, it should not throw an error; a key will " +
                "just not be generated");
        });
    }

    @Test
    void noRowsReturned() throws InterruptedException {
        List<SourceRecord> records = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.ROW_LIMIT, "1000"
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
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.ROW_LIMIT, "1000"
        ).poll();

        assertEquals(15, records.size());

        recordsToJsonObjects(records).forEach(row -> assertEquals(2, row.size(), "Expecting 2 columns for LastName and ForeName"));
    }

    @Test
    void fromDocUris() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        List<SourceRecord> records = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, "op.fromDocUris(cts.documentQuery('citations.xml')).joinDoc(op.col('doc'), op.col('uri'))",
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.ROW_LIMIT, "1000"
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
            MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.JSON.toString(),
            MarkLogicSourceConfig.ROW_LIMIT, "1000"
        ).poll();

        assertNull(records, "Should get null back when no rows match; also, check the logging to ensure that " +
            "no exception was thrown");
    }

    @Test
    void pullDataUsingFromSearch() throws IOException {
        loadThreeAuthorDocuments();

        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "score");
            put(MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.JSON.toString());
            put(MarkLogicSourceConfig.DSL_QUERY, "op.fromSearch(cts.wordQuery('61296'))");
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};

        verifyQueryReturnsExpectedRows(null, 3, "score", parsedConfig);
    }

    @Test
    void pullDataUsingFromSearchDocs() throws IOException {
        loadThreeAuthorDocuments();

        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "uri");
            put(MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.JSON.toString());
            put(MarkLogicSourceConfig.DSL_QUERY, "op.fromSearchDocs(cts.wordQuery('61296'))");
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};

        verifyQueryReturnsExpectedRows(null, 3, "score", parsedConfig);
    }

    @Test
    void pullDataUsingFromSPARQL() {
        loadFifteenAuthorsIntoMarkLogic();

        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "o");
            put(MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.JSON.toString());
            put(MarkLogicSourceConfig.DSL_QUERY,
                "op.fromSPARQL('PREFIX authors: <http://marklogic.com/column/Medical/Authors/> SELECT * WHERE { ?s authors:Date ?o } ')");
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};

        verifyQueryReturnsExpectedRows(null, 15, "xs:date", parsedConfig);
    }

    @Test
    void pullDataUsingFromTriples() {
        loadFifteenAuthorsIntoMarkLogic();

        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "ISSN");
            put(MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.JSON.toString());
            put(MarkLogicSourceConfig.DSL_QUERY,
                "op.fromTriples([op.pattern(sem.iri('http://marklogic.com/example/person/_'), sem.iri('http://marklogic.com/example/authored'),op.col('ISSN'))])");
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};

        verifyQueryReturnsExpectedRows(null, 15, "ISSN", parsedConfig);
    }
}
