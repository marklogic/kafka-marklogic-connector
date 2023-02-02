/*
 * Copyright (c) 2023 MarkLogic Corporation
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

import static org.junit.jupiter.api.Assertions.assertNull;

class ReadRowsViaOpticSerializedTest extends AbstractIntegrationSourceTest {

    @Test
    void readFifteenAuthors() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        RowManagerSourceTask task = startSourceTask(
            MarkLogicSourceConfig.SERIALIZED_QUERY, AUTHORS_OPTIC_SERIALIZED,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

        List<SourceRecord> newSourceRecords = task.poll();
        verifyQueryReturnsFifteenAuthors(newSourceRecords, ReadRowsViaOpticDslTest.JSON_RESULT);
    }

    @Test
    void noRowsReturned() throws InterruptedException {
        List<SourceRecord> records = startSourceTask(
            MarkLogicSourceConfig.SERIALIZED_QUERY, AUTHORS_OPTIC_SERIALIZED,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        ).poll();

        assertNull(records, "When no rows exist, null should be returned; an exception should not be thrown. " +
            "This ensures that bug https://bugtrack.marklogic.com/58240 does not cause an error when a user instead " +
            "expects no data to be returned.");
    }
}
