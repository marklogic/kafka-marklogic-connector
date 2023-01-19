package com.marklogic.kafka.connect.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ReadXmlRowsTest extends AbstractIntegrationSourceTest {
    private final String XML_RESULT = "<t:row xmlns:t=\"http://marklogic.com/table\">\n" +
        "<t:cell name=\"Medical.Authors.ID\" type=\"xs:integer\">2</t:cell>\n" +
        "<t:cell name=\"Medical.Authors.LastName\" type=\"xs:string\">Pulhoster</t:cell>\n" +
        "<t:cell name=\"Medical.Authors.ForeName\" type=\"xs:string\">Misty</t:cell>\n" +
        "<t:cell name=\"Medical.Authors.Date\" type=\"xs:date\">2022-05-11</t:cell>\n" +
        "<t:cell name=\"Medical.Authors.DateTime\" type=\"xs:dateTime\">2022-05-11T10:00:00</t:cell>\n" +
        "</t:row>";

    @Test
    void readFifteenAuthorsAsXml() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        RowManagerSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL + ".orderBy(op.asc(op.col('ID')))",
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.XML.toString(),
            MarkLogicSourceConfig.KEY_COLUMN, "Medical.Authors.ID"
        );

        List<SourceRecord> newSourceRecords = task.poll();
        verifyQueryReturnsFifteenAuthors(newSourceRecords, XML_RESULT);

        assertEquals("1", newSourceRecords.get(0).key(), "The key should be populated by the ID column");
        assertEquals("5", newSourceRecords.get(14).key());
    }

    @Test
    void keyColumnDoesntExist() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        RowManagerSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.XML.toString(),
            MarkLogicSourceConfig.KEY_COLUMN, "column-doesnt-exist"
        );

        List<SourceRecord> sourceRecords = task.poll();
        verifyQueryReturnsFifteenAuthors(sourceRecords, XML_RESULT);

        sourceRecords.forEach(sourceRecord -> {
            assertNull(sourceRecord.key(), "If the column is not found, it should not throw an error; a key will " +
                "just not be generated");
        });
    }

    @Test
    void noMatchingRows() throws InterruptedException {
        List<SourceRecord> records = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, "op.fromDocUris(cts.documentQuery('no-such-document'))",
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.XML.toString()
        ).poll();

        assertNull(records, "Should get null back when no rows match; also, check the logging to ensure that " +
            "no exception was thrown");
    }
}
