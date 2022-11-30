package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.datamovement.RowBatcher;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.FileHandle;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadRowsViaOpticDslTest extends AbstractIntegrationSourceTest {

    @BeforeEach
    void setup() {
        loadMarkLogicTestData();
    }

    @Test
    void testRowBatcherTask() throws InterruptedException {

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
    void testDslThatDoesNotStartWithFromView() {
        String fromSqlDsl = "op.fromSQL('SELECT employees.FirstName, employees.LastName FROM employees')";
        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_PLAN, fromSqlDsl,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );
        RuntimeException ex = assertThrows(RuntimeException.class, () -> task.getNewRowBatcher(new Vector<>()));
        task.stop();

        assertTrue(ex.getMessage().startsWith("Unable to poll for source records;"), "Unexpected message: " + ex);
    }

    @Test
    void testBatcherStopDoesNotWaitForCompletion() throws InterruptedException {
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
        docMgr.write("citations.xml", new FileHandle(new File("src/test/resources/citations.xml")));
    }
}
