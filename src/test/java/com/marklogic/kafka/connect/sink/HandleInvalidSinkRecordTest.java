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
