package com.marklogic.kafka.connect.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ReadCsvRowsTest extends AbstractIntegrationSourceTest {
    protected final String CSV_RESULT = "Medical.Authors.ID,Medical.Authors.LastName,Medical.Authors.ForeName,Medical.Authors.Date,Medical.Authors.DateTime\n" +
        "2,Pulhoster,Misty,2022-05-11,2022-05-11T10:00:00";

    @Test
    void readFifteenAuthorsAsCsv() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        RowManagerSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL + ".orderBy(op.asc(op.col('ID')))",
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.CSV.toString(),
            MarkLogicSourceConfig.KEY_COLUMN, "Medical.Authors.ID"
        );

        List<SourceRecord> newSourceRecords = task.poll();
        verifyQueryReturnsFifteenAuthors(newSourceRecords, CSV_RESULT);

        assertEquals("1", newSourceRecords.get(0).key(), "The key should be populated by the ID column");
        assertEquals("5", newSourceRecords.get(14).key());
    }

    @Test
    void keyColumnDoesntExist() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        RowManagerSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.CSV.toString(),
            MarkLogicSourceConfig.KEY_COLUMN, "column-doesnt-exist"
        );

        List<SourceRecord> sourceRecords = task.poll();
        verifyQueryReturnsFifteenAuthors(sourceRecords, CSV_RESULT);

        sourceRecords.forEach(sourceRecord -> {
            assertNull(sourceRecord.key(), "If the column is not found, it should not throw an error; a key will " +
                "just not be generated");
        });
    }

    @Test
    void noMatchingRows() throws InterruptedException {
        RowManagerSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.CSV.toString()
        );

        List<SourceRecord> records = task.poll();
        assertNull(records, "Should get null back when no rows match; also, check the logging to ensure that " +
            "no exception was thrown");
    }

}
