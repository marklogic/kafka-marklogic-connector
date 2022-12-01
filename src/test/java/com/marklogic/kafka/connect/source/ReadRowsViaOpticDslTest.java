package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.datamovement.RowBatcher;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.FileHandle;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ReadRowsViaOpticDslTest extends AbstractIntegrationSourceTest {

    @Test
    void testRowBatcherTask() throws InterruptedException {
        loadMarkLogicTestData();

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_PLAN, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

        List<SourceRecord> newSourceRecords = task.poll();

        Assertions.assertEquals(15, newSourceRecords.size());
        assertTopicAndSingleValue(newSourceRecords, AUTHORS_TOPIC);
    }

    @Test
    void noRowsReturned() throws InterruptedException {
        List<SourceRecord> records = startSourceTask(
            MarkLogicSourceConfig.DSL_PLAN, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        ).poll();

        assertNull(records, "When no rows exist, null should be returned; an exception should not be thrown. " +
            "This ensures that bug https://bugtrack.marklogic.com/58240 does not cause an error when a user instead " +
            "expects no data to be returned.");
    }

    @Test
    void testDslThatDoesNotStartWithFromView() {
        String fromSqlDsl = "op.fromSQL('SELECT employees.FirstName, employees.LastName FROM employees')";
        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_PLAN, fromSqlDsl,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

        RuntimeException ex = assertThrows(RuntimeException.class, task::poll);

        assertTrue(ex.getMessage().startsWith("Unable to poll for source records; cause: "),
            "Unexpected message: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("First operation in Optic plan must be fromView()"),
            "Unexpected message: " + ex.getMessage());
    }

    @Test
    void testBatcherStopDoesNotWaitForCompletion() throws InterruptedException {
        loadMarkLogicTestData();

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_PLAN, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );
        List<SourceRecord> newSourceRecords = new Vector<>();
        RowBatcher<JsonNode> rowBatcher = task.getNewRowBatcher(newSourceRecords);

        // Register our own success listener to look for any onSuccess events
        // and set a variable tracking onSuccess events.
        AtomicReference<Boolean> onSuccessCalled = new AtomicReference<>(false);
        rowBatcher.onSuccess(event -> {
            onSuccessCalled.set(true);
        });

        // Start a new thread that can be paused before polling
        // to verify that task.stop() prevents any new polling.
        Thread t1 = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            task.performPoll();
        });
        t1.start();

        task.stop();
        Assertions.assertFalse(onSuccessCalled.get(),
            "RowBatcherSourceTask should have stopped before any onSuccess events");
        Assertions.assertEquals(0, newSourceRecords.size());
    }

    private void assertTopicAndSingleValue(List<SourceRecord> newSourceRecords, String topic) {
        String expectedValue = "{\"Medical.Authors.ID\":{\"type\":\"xs:integer\",\"value\":2}," +
            "\"Medical.Authors.LastName\":{\"type\":\"xs:string\",\"value\":\"Pulhoster\"}," +
            "\"Medical.Authors.ForeName\":{\"type\":\"xs:string\",\"value\":\"Misty\"}}";
        AtomicReference<Boolean> foundExpectedValue = new AtomicReference<>(false);
        newSourceRecords.forEach(sourceRecord -> {
            Assertions.assertEquals(topic, sourceRecord.topic());
            System.out.println(sourceRecord.value());
            if (expectedValue.equals(sourceRecord.value().toString())) {
                foundExpectedValue.set(true);
            }
        });
        Assertions.assertTrue(foundExpectedValue.get(),
            "List of SourceRecords does not contain a record with the expected value");
    }

    private void loadMarkLogicTestData() {
        XMLDocumentManager docMgr = getDatabaseClient().newXMLDocumentManager();
        docMgr.write("citations.xml",
            new DocumentMetadataHandle().withPermission("kafka-test-minimal-user", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE),
            new FileHandle(new File("src/test/resources/citations.xml")));
    }
}
