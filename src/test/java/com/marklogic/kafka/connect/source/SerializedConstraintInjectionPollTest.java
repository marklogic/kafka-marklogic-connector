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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SerializedConstraintInjectionPollTest extends AbstractIntegrationSourceTest {

    @Test
    void testMaxValueRetrievalWithConstraintInjection() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();
        String constraintColumnName = "ID";
        String limitedAuthorsSerialized = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}]}}";

        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.SERIALIZED_QUERY, limitedAuthorsSerialized,
                MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumnName,
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
                MarkLogicSourceConfig.ROW_LIMIT, "3");

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
                int recordId = authorJson.findPath("Medical.Authors.ID").intValue();
                Assertions.assertTrue(recordId <= firstMaxConstraintColumnValueInteger,
                        recordId + " should be less than or equal to " + firstMaxConstraintColumnValueInteger);
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
                int recordId = authorJson.findPath("Medical.Authors.ID").intValue();
                Assertions.assertTrue(recordId > firstMaxConstraintColumnValueInteger,
                        recordId + " should be less than or equal to " + firstMaxConstraintColumnValueInteger);
                Assertions.assertTrue(recordId <= secondMaxConstraintColumnValueInteger,
                        recordId + " should be less than or equal to " + secondMaxConstraintColumnValueInteger);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void testMaxValueRetrievalWithDateConstraint() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();
        String constraintColumnName = "Date";
        String limitedAuthorsSerialized = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}]}}";

        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.SERIALIZED_QUERY, limitedAuthorsSerialized,
                MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumnName,
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
                MarkLogicSourceConfig.ROW_LIMIT, "3");

        task.poll();
        String previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals("2022-04-11", previousMaxConstraintColumnValue,
                "The max value after polling once should be '\"2022-04-11\"'");

        task.poll();
        previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals("2022-05-11", previousMaxConstraintColumnValue,
                "The max value after polling twice should be '2022-05-11'");
    }

    @Test
    void testMaxValueRetrievalWithDateTimeConstraint() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();
        String constraintColumnName = "DateTime";
        String limitedAuthorsSerialized = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}]}}";

        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.SERIALIZED_QUERY, limitedAuthorsSerialized,
                MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumnName,
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
                MarkLogicSourceConfig.ROW_LIMIT, "3");

        task.poll();
        String previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals("2022-04-11T11:00:00", previousMaxConstraintColumnValue,
                "The max value after polling once should be '2022-04-11T11:00:00'");

        task.poll();
        previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals("2022-05-11T10:00:00", previousMaxConstraintColumnValue,
                "The max value after polling twice should be '2022-05-11T10:00:00'");
    }

    @Test
    void testSecondPollWithNewRowsOnlyReturnsRowAfterPreviousBatchMaxValue() throws IOException, InterruptedException {
        String constraintColumnName = "DateTime";
        String limitedAuthorsSerialized = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}, {\"ns\":\"op\", \"fn\":\"order-by\", \"args\":[{\"ns\":\"op\", \"fn\":\"asc\", \"args\":[\""
                + constraintColumnName + "\"]}]}, {\"ns\":\"op\", \"fn\":\"limit\", \"args\":[3]}]}}";

        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.SERIALIZED_QUERY, limitedAuthorsSerialized,
                MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumnName,
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC);

        // 1) Insert data that will return a row and has a dateTime column in it
        // (exposed in the TDE);
        String baseDate = "2022-07-13T";
        String initialTime = "01:01:00";
        String initialDateTime = baseDate + initialTime;
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("firstTime", "6", initialTime, "Initial");

        // 2) Run the connector, verify the row is returned;
        List<SourceRecord> newRecords = task.poll();
        String previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals(initialDateTime, previousMaxConstraintColumnValue,
                "The max value after polling once should be equal to the initial time");
        Assertions.assertEquals(1, newRecords.size());
        String initialRow = (String) newRecords.get(0).value();
        Assertions.assertTrue(initialRow.contains("Initial"), "Did not find 'Initial' in: " + initialRow);

        // 3) Insert a second document with a dateTime greater than that of the first
        // row,
        String laterTime = "02:01:00";
        String laterDateTime = baseDate + laterTime;
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("later", "7", laterTime, "Later");

        // 4) and insert a third document with a dateTime less than that of the first
        // row;
        String earlierTime = "00:01:00";
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("earlier", "8", earlierTime, "Earlier");

        // 5) Run the connector again,
        newRecords = task.poll();

        // 6) verify that only the second document is returned
        previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        Assertions.assertEquals(laterDateTime, previousMaxConstraintColumnValue,
                "The max value after polling once should be equal to the initial time");
        Assertions.assertEquals(1, newRecords.size());
        String laterRow = (String) newRecords.get(0).value();
        Assertions.assertTrue(laterRow.contains("Later"), "Did not find 'Later' in: " + laterRow);
    }

    @Test
    void constraintColumnNameIsEmptyString() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();
        String limitedAuthorsSerialized = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}]}}";

        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.SERIALIZED_QUERY, limitedAuthorsSerialized,
                MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "",
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC);

        assertEquals(15, task.poll().size());
        assertEquals(15, task.poll().size(), "An empty string for the constraint column name should be treated like " +
                "null, such that it's not applied and we thus get back 15 rows each time.");
    }
}
