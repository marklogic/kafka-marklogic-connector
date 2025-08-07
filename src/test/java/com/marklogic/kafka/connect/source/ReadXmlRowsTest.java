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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ReadXmlRowsTest extends AbstractIntegrationSourceTest {

    private final String XML_RESULT = "<t:row xmlns:t=\"http://marklogic.com/table\">\n" +
            "<t:cell name=\"Medical.Authors.ID\">2</t:cell>\n" +
            "<t:cell name=\"Medical.Authors.LastName\">Pulhoster</t:cell>\n" +
            "<t:cell name=\"Medical.Authors.ForeName\">Misty</t:cell>\n" +
            "<t:cell name=\"Medical.Authors.Date\">2022-05-11</t:cell>\n" +
            "<t:cell name=\"Medical.Authors.DateTime\">2022-05-11T10:00:00</t:cell>\n" +
            "</t:row>";

    private final String XML_RESULT_WITH_COLUMN_TYPES = "<t:row xmlns:t=\"http://marklogic.com/table\">\n" +
            "<t:cell name=\"Medical.Authors.ID\" type=\"xs:integer\">2</t:cell>\n" +
            "<t:cell name=\"Medical.Authors.LastName\" type=\"xs:string\">Pulhoster</t:cell>\n" +
            "<t:cell name=\"Medical.Authors.ForeName\" type=\"xs:string\">Misty</t:cell>\n" +
            "<t:cell name=\"Medical.Authors.Date\" type=\"xs:date\">2022-05-11</t:cell>\n" +
            "<t:cell name=\"Medical.Authors.DateTime\" type=\"xs:dateTime\">2022-05-11T10:00:00</t:cell>\n" +
            "</t:row>";

    @BeforeEach
    void setup() {
        loadFifteenAuthorsIntoMarkLogic();
    }

    @Test
    void readFifteenAuthorsAsXml() throws InterruptedException {
        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.DSL_QUERY, AUTHORS_ORDERED_BY_ID_OPTIC_DSL,
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
                MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.XML.toString(),
                MarkLogicSourceConfig.KEY_COLUMN, "Medical.Authors.ID");

        List<SourceRecord> records = task.poll();
        verifyQueryReturnsFifteenAuthors(records, XML_RESULT);
        verifyRecordKeysAreSetToIDColumn(records);
    }

    @Test
    void includeColumnTypes() throws InterruptedException {
        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.DSL_QUERY, AUTHORS_ORDERED_BY_ID_OPTIC_DSL,
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
                MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.XML.toString(),
                MarkLogicSourceConfig.INCLUDE_COLUMN_TYPES, "true",
                MarkLogicSourceConfig.KEY_COLUMN, "Medical.Authors.ID");

        List<SourceRecord> records = task.poll();
        verifyQueryReturnsFifteenAuthors(records, XML_RESULT_WITH_COLUMN_TYPES);
        verifyRecordKeysAreSetToIDColumn(records);
    }

    @Test
    void keyColumnDoesntExist() throws InterruptedException {
        RowManagerSourceTask task = startSourceTask(
                MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
                MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.XML.toString(),
                MarkLogicSourceConfig.KEY_COLUMN, "column-doesnt-exist");

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
                MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.XML.toString()).poll();

        assertNull(records, "Should get null back when no rows match; also, check the logging to ensure that " +
                "no exception was thrown");
    }
}
