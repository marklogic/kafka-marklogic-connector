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
package com.marklogic.kafka.connect.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class HandleInvalidSinkRecordTest extends AbstractIntegrationSinkTest {

    @Test
    void sinkRecordWithNullContent() {
        final String collection = "empty-test";
        AbstractSinkTask task = startSinkTask(
            MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, collection
        );

        putAndFlushRecords(task, newSinkRecord(null));
        assertCollectionSize("Kafka is not expected to send a record with null content, but in case this happens " +
            "somehow, the record should be ignored", collection, 0);
    }

    @Test
    void nullSinkRecord() {
        final String collection = "null-test";
        AbstractSinkTask task = startSinkTask(
            MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, collection
        );

        List<SinkRecord> list = new ArrayList<>();
        list.add(null);
        task.put(list);
        task.flush(null);
        assertCollectionSize("Kafka is not expected to send a null record, but in case this happens " +
            "somehow, the record should be ignored", collection, 0);
    }
}
