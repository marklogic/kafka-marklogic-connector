package com.marklogic.kafka.connect.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

class ReadXmlRowsTest extends AbstractIntegrationSourceTest {
    protected final String XML_RESULT = "<t:row xmlns:t=\"http://marklogic.com/table\">\n" +
        "<t:cell name=\"Medical.Authors.ID\" type=\"xs:integer\">2</t:cell>\n" +
        "<t:cell name=\"Medical.Authors.LastName\" type=\"xs:string\">Pulhoster</t:cell>\n" +
        "<t:cell name=\"Medical.Authors.ForeName\" type=\"xs:string\">Misty</t:cell>\n" +
        "</t:row>";

    @Test
    void testRowBatcherTask() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.XML.toString()
        );

        List<SourceRecord> newSourceRecords = task.poll();
        verifyQueryReturnsFifteenAuthors(newSourceRecords, XML_RESULT);
    }
}