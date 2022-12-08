package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.kafka.connect.source.MarkLogicSourceConfig;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FailedConversionErrorReporterTest extends AbstractIntegrationSinkTest {

    Throwable reportedException = null;
    SinkRecord reportedSinkRecord = null;
    final private String someValidJsonToSignalAContrivedException = "{ \"A\": \"a\"}";
    final private String targetCollection = "good-data";

    @Test
    void sinkRecordWithNullValueCausesCallToErrorReporter() {
        String someOtherValidJson = "{ \"B\": \"b\"}";
        WriteBatcherSinkTask task = (WriteBatcherSinkTask) startSinkTask(
            (SinkRecord record, Throwable e) -> { reportedSinkRecord = record; reportedException = e; },
            MarkLogicSinkConfig.DOCUMENT_FORMAT, "json",
            MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_METADATA, "true",
            MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, targetCollection
        );
        task.setErrorReporterMethod((SinkRecord record, Throwable e) -> { reportedSinkRecord = record; reportedException = e; });
        task.setSinkRecordConverter(new ContrivedSinkRecordConverter(getTaskConfig()));

        String targetTopic = "someTopic";
        SinkRecord badRecord = new SinkRecord(targetTopic, 1, null, null,
            null, someValidJsonToSignalAContrivedException, 123);
        SinkRecord goodRecord = new SinkRecord(targetTopic, 1, null, null,
            null, someOtherValidJson, 123);
        putAndFlushRecords(task, badRecord, goodRecord);

        assertCollectionSize(targetCollection, 1);
        Assertions.assertNotNull(reportedException,
            "The mock error reporter should have been called with an exception");
        assertEquals("Contrived Exception for testing", reportedException.getMessage(),
            "The reported exception message does not match the expected message");
        Headers headers = reportedSinkRecord.headers();
        assertEquals(3, headers.size());
        assertEquals(AbstractSinkTask.MARKLOGIC_CONVERSION_FAILURE, headers.lastWithName(AbstractSinkTask.MARKLOGIC_MESSAGE_FAILURE_HEADER).value());
        assertEquals("Contrived Exception for testing", headers.lastWithName(AbstractSinkTask.MARKLOGIC_MESSAGE_EXCEPTION_MESSAGE).value());
        assertEquals(targetTopic, headers.lastWithName(AbstractSinkTask.MARKLOGIC_ORIGINAL_TOPIC).value());
    }

    class ContrivedSinkRecordConverter extends DefaultSinkRecordConverter {

        public ContrivedSinkRecordConverter(Map<String, Object> parsedConfig) {
            super(parsedConfig);
        }

        @Override
        public DocumentWriteOperation convert(SinkRecord sinkRecord) {
            if (sinkRecord.value().equals(someValidJsonToSignalAContrivedException)) {
                throw new RuntimeException("Contrived Exception for testing");
            }
            return super.convert(sinkRecord);
        }
    }
}
