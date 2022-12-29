package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

class DslConstraintInjectionPollTest extends AbstractIntegrationSourceTest {

    @Test
    void testMaxValueRetrievalWithConstraintInjection() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();
        String constraintColumnName = "ID";
        String limitedAuthorsDsl = AUTHORS_OPTIC_DSL + ".orderBy(op.asc(\"" + constraintColumnName + "\")).limit(3)";

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, String.valueOf(Integer.MAX_VALUE),
            MarkLogicSourceConfig.DSL_QUERY, limitedAuthorsDsl,
            MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumnName,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

        List<SourceRecord> newRecords = task.poll();
        String previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals("1", previousMaxConstraintColumnValue,
            "The max value after polling once should be '1'");
        Assertions.assertEquals(3, newRecords.size(), "Each poll() should only return 3 records");
        int firstMaxConstraintColumnValueInteger = Integer.parseInt(previousMaxConstraintColumnValue);
        ObjectMapper mapper = new ObjectMapper();
        newRecords.forEach(sourceRecord -> {
            try {
                JsonNode authorJson = mapper.readTree((String) sourceRecord.value());
                int recordId = authorJson.findPath("Medical.Authors.ID").findPath("value").intValue();
                Assertions.assertTrue(recordId <= firstMaxConstraintColumnValueInteger);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });

        newRecords = task.poll();
        previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals("2", previousMaxConstraintColumnValue,
            "The max value after polling twice should be '2'");
        Assertions.assertEquals(3, newRecords.size(), "Each poll() should only return 3 records");
        int secondMaxConstraintColumnValueInteger = Integer.parseInt(previousMaxConstraintColumnValue);
        newRecords.forEach(sourceRecord -> {
            try {
                JsonNode authorJson = mapper.readTree((String) sourceRecord.value());
                int recordId = authorJson.findPath("Medical.Authors.ID").findPath("value").intValue();
                Assertions.assertTrue(recordId > firstMaxConstraintColumnValueInteger);
                Assertions.assertTrue(recordId <= secondMaxConstraintColumnValueInteger);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void testMaxValueRetrievalWithDateConstraint() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();
        String constraintColumnName = "Date";
        String limitedAuthorsDsl = AUTHORS_OPTIC_DSL + ".orderBy(op.asc(\"" + constraintColumnName + "\")).limit(3)";

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_QUERY, limitedAuthorsDsl,
            MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumnName,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

        task.poll();
        String previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals("2022-04-11", previousMaxConstraintColumnValue,
            "The max value after polling once should be '\"2022-04-11\"'");

        task.poll();
        previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals("2022-05-11", previousMaxConstraintColumnValue,
            "The max value after polling twice should be '\"2022-05-11\"'");
    }

    @Test
    void testMaxValueRetrievalWithDateTimeConstraint() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();
        String constraintColumnName = "DateTime";
        String limitedAuthorsDsl = AUTHORS_OPTIC_DSL + ".orderBy(op.asc(\"" + constraintColumnName + "\")).limit(3)";

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_QUERY, limitedAuthorsDsl,
            MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumnName,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

        task.poll();
        String previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals("2022-04-11T11:00:00", previousMaxConstraintColumnValue,
            "The max value after polling once should be '\"2022-04-11T11:00:00\"'");

        task.poll();
        previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals("2022-05-11T10:00:00", previousMaxConstraintColumnValue,
            "The max value after polling twice should be '\"2022-05-11T10:00:00\"'");
    }

    @Test
    void testSecondPollWithNewRowsOnlyReturnsRowAfterPreviousBatchMaxValue() throws IOException, InterruptedException {
        String constraintColumnName = "DateTime";
        String authorsDsl = AUTHORS_OPTIC_DSL + ".orderBy(op.asc(\"" + constraintColumnName + "\"))";

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_QUERY, authorsDsl,
            MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumnName,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

        //    1) Insert data that will return a row and has a dateTime column in it (exposed in the TDE);
        String baseDate = "2022-07-13T";
        String initialTime = "01:01:00";
        String initialDateTime = baseDate + initialTime;
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("firstTime", initialTime, "Initial");

        //    2) Run the connector, verify the row is returned;
        List<SourceRecord> newRecords = task.poll();
        String previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals(initialDateTime, previousMaxConstraintColumnValue,
            "The max value after polling once should be equal to the initial time");
        Assertions.assertEquals(1, newRecords.size());
        String initialRow = (String) newRecords.get(0).value();
        Assertions.assertTrue(initialRow.contains("Initial"));

        //    3) Insert a second document with a dateTime greater than that of the first row,
        String laterTime = "02:01:00";
        String laterDateTime = baseDate + laterTime;
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("later", laterTime, "Later");

        //    4) and insert a third document with a dateTime less than that of the first row;
        String earlierTime = "00:01:00";
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("earlier", earlierTime, "Earlier");

        //    5) Run the connector again,
        newRecords = task.poll();

        //    6) verify that only the second document is returned
        previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals(laterDateTime, previousMaxConstraintColumnValue,
            "The max value after polling once should be equal to the initial time");
        Assertions.assertEquals(1, newRecords.size());
        String laterRow = (String) newRecords.get(0).value();
        Assertions.assertTrue(laterRow.contains("Later"));
    }
}