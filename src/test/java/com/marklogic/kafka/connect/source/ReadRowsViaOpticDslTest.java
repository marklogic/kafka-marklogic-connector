package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.datamovement.RowBatcher;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ReadRowsViaOpticDslTest extends AbstractIntegrationSourceTest {

    @Test
    void testRowBatcherTask() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

        List<SourceRecord> newSourceRecords = task.poll();
        verifyQueryReturnsFifteenAuthors(newSourceRecords);
    }

    @Test
    void noRowsReturned() throws InterruptedException {
        List<SourceRecord> records = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        ).poll();

        assertNull(records, "When no rows exist, null should be returned; an exception should not be thrown. " +
            "This ensures that bug https://bugtrack.marklogic.com/58240 does not cause an error when a user instead " +
            "expects no data to be returned.");
    }

    @Test
    void testDslThatDoesNotStartWithFromView() throws InterruptedException {
        String fromSqlDsl = "op.fromSQL('SELECT employees.FirstName, employees.LastName FROM employees')";
        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_QUERY, fromSqlDsl,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );
        assertNull(task.poll());
    }

    @Test
    void testBatcherStopDoesNotWaitForCompletion() {
        loadFifteenAuthorsIntoMarkLogic();

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );
        List<SourceRecord> newSourceRecords = new Vector<>();
        RowBatcher<JsonNode> rowBatcher = task.newRowBatcher(newSourceRecords);

        // Register our own success listener to look for any onSuccess events
        // and set a variable tracking onSuccess events.
        AtomicReference<Boolean> onSuccessCalled = new AtomicReference<>(false);
        rowBatcher.onSuccess(event -> onSuccessCalled.set(true));

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
}
