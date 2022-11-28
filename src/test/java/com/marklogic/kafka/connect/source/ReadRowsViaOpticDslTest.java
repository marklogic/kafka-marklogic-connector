package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.datamovement.RowBatcher;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.FileHandle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

class ReadRowsViaOpticDslTest extends AbstractIntegrationSourceTest {

    @BeforeEach
    void setup() {
        loadMarkLogicTestData();
    }

    @Test
    void testRowBatcherTask() {

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_PLAN, AUTHORS_OPTIC_DSL
        );

        // Register our own success listener to inspect the batch
        AtomicReference<Integer> rowCount = new AtomicReference<>(0);
        RowBatcher<JsonNode> rowBatcher = task.getNewRowBatcher();
        rowBatcher.onSuccess(event -> {
            JsonNode rows = event.getRowsDoc().get("rows");
            rowCount.updateAndGet(v -> v + rows.size());
        });
        rowBatcher.onFailure((batch, throwable) -> Assertions.fail("RowBatcherSourceTask failed to execute properly"));

        task.performPoll();
        Assertions.assertEquals(15, rowCount.get());
    }

    @Test
    void testBatcherStopDoesNotWaitForCompletion() throws InterruptedException {
        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DMSDK_BATCH_SIZE, "1",
            MarkLogicSourceConfig.DSL_PLAN, AUTHORS_OPTIC_DSL
        );
        RowBatcher<JsonNode> rowBatcher = task.getNewRowBatcher();

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
    }

    private void loadMarkLogicTestData() {
        XMLDocumentManager docMgr = getDatabaseClient().newXMLDocumentManager();
        docMgr.write("citations.xml", new FileHandle(new File("src/test/resources/citations.xml")));
    }
}
