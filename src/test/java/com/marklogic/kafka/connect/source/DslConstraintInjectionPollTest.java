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

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DslConstraintInjectionPollTest extends AbstractIntegrationSourceTest {

    @Test
    void orderAuthorsByIDNumberAndLimit3() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();
        String constraintColumnName = "ID";
        String limitedAuthorsDsl = AUTHORS_OPTIC_DSL + ".orderBy(op.asc(op.col('" + constraintColumnName + "')))";

        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.DSL_QUERY, limitedAuthorsDsl,
                MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumnName,
                MarkLogicSourceConfig.ROW_LIMIT, "3",
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC);

        List<SourceRecord> newRecords = task.poll();
        String previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        assertEquals("1", previousMaxConstraintColumnValue, "The lowest ID of the first 3 authors sorted by ID is 1");
        assertEquals(3, newRecords.size(), "Each poll() should only return 3 records due to the use of limit()");
        recordsToJsonObjects(newRecords).forEach(author -> {
            int recordId = author.findPath("Medical.Authors.ID").intValue();
            assertEquals(1, recordId);
        });

        newRecords = task.poll();
        previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        assertEquals("2", previousMaxConstraintColumnValue, "The query should have omitted authors with an ID of 1 " +
                "and returned the next batch of 3, which are all expected to have an ID of 2");
        assertEquals(3, newRecords.size());
        recordsToJsonObjects(newRecords).forEach(author -> {
            int recordId = author.findPath("Medical.Authors.ID").intValue();
            assertEquals(2, recordId);
        });
    }

    @Test
    void orderAuthorsByDateAndLimit3() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();
        String constraintColumnName = "Date";
        String limitedAuthorsDsl = AUTHORS_OPTIC_DSL + ".orderBy(op.asc(op.col('" + constraintColumnName + "')))";

        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.DSL_QUERY, limitedAuthorsDsl,
                MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumnName,
                MarkLogicSourceConfig.ROW_LIMIT, "3",
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC);

        List<SourceRecord> records = task.poll();
        assertEquals(3, records.size());
        assertEquals("2022-04-11", task.getPreviousMaxConstraintColumnValue(),
                "Did not get the expected 3rd lowest date");

        records = task.poll();
        assertEquals(3, records.size());
        assertEquals("2022-05-11", task.getPreviousMaxConstraintColumnValue(),
                "Did not get the expected 6th lowest date");
    }

    @Test
    void orderAuthorsByDateTimeAndLimit3() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();
        String constraintColumnName = "DateTime";
        String limitedAuthorsDsl = AUTHORS_OPTIC_DSL + ".orderBy(op.asc(op.col('" + constraintColumnName + "')))";

        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.DSL_QUERY, limitedAuthorsDsl,
                MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumnName,
                MarkLogicSourceConfig.ROW_LIMIT, "3",
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC);

        List<SourceRecord> records = task.poll();
        assertEquals(3, records.size());
        String previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        assertEquals("2022-04-11T11:00:00", previousMaxConstraintColumnValue,
                "Did not get the expected 3rd lowest dateTime");

        records = task.poll();
        assertEquals(3, records.size());
        previousMaxConstraintColumnValue = task.getPreviousMaxConstraintColumnValue();
        assertEquals("2022-05-11T10:00:00", previousMaxConstraintColumnValue,
                "Did not get the expected 6th lowest dateTime");
    }

    @Test
    void insertNewDataBeforeSecondPoll() throws Exception {
        String constraintColumnName = "DateTime";
        String authorsDsl = AUTHORS_OPTIC_DSL + ".orderBy(op.asc(op.col('" + constraintColumnName + "')))";

        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.DSL_QUERY, authorsDsl,
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
        assertEquals(initialDateTime, previousMaxConstraintColumnValue,
                "The max value after polling once should be equal to the initial time");
        assertEquals(1, newRecords.size());
        String initialRow = (String) newRecords.get(0).value();
        assertTrue(initialRow.contains("Initial"), "Did not find 'Initial' in: " + initialRow);

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
        assertEquals(laterDateTime, previousMaxConstraintColumnValue,
                "The max value after polling once should be equal to the initial time");
        assertEquals(1, newRecords.size());
        String laterRow = (String) newRecords.get(0).value();
        assertTrue(laterRow.contains("Later"), "Did not find 'Later' in: " + laterRow);
    }

    @Test
    void secondPollReturnsNoRows() throws Exception {
        loadFifteenAuthorsIntoMarkLogic();

        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
                MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "ID",
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC);

        List<SourceRecord> records = task.poll();
        assertEquals(15, records.size());
        assertEquals("5", task.getPreviousMaxConstraintColumnValue(),
                "The highest expected ID of the fifteen authors is 5");

        records = task.poll();
        assertNull(records, "Since no new rows exist with an ID greater than 5, null should be " +
                "returned to indicate there's no new data. Additionally, manual inspection of the logs should show that " +
                "no attempt was made by the task to fetch a new max value since the second poll returned no rows.");
    }

    @Test
    void constraintColumnNameIsEmptyString() throws Exception {
        loadFifteenAuthorsIntoMarkLogic();

        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
                MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "",
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC);

        assertEquals(15, task.poll().size());
        assertEquals(15, task.poll().size(), "An empty string for the constraint column name should be treated like " +
                "null, such that it's not applied and we thus get back 15 rows each time.");
    }
}
