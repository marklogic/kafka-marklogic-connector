package com.marklogic.kafka.connect.source;

import com.marklogic.client.datamovement.RowBatcher;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ReadRowsViaOpticDslTest extends AbstractIntegrationSourceTest {
    protected static final String JSON_RESULT =
        "{\"Medical.Authors.ID\":{\"type\":\"xs:integer\",\"value\":2}," +
        "\"Medical.Authors.LastName\":{\"type\":\"xs:string\",\"value\":\"Pulhoster\"}," +
        "\"Medical.Authors.ForeName\":{\"type\":\"xs:string\",\"value\":\"Misty\"}," +
        "\"Medical.Authors.Date\":{\"type\":\"xs:date\",\"value\":\"2022-05-11\"}," +
        "\"Medical.Authors.DateTime\":{\"type\":\"xs:dateTime\",\"value\":\"2022-05-11T10:00:00\"}}";

    @Test
    void testRowBatcherTask() throws InterruptedException {
        loadFifteenAuthorsIntoMarkLogic();

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

        List<SourceRecord> newSourceRecords = task.poll();
        verifyQueryReturnsFifteenAuthors(newSourceRecords, JSON_RESULT);
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
            MarkLogicSourceConfig.DSL_QUERY, fromSqlDsl,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );
        Assertions.assertNull(task.poll(), "Invalid DSL should cause poll() to fail");
    }

    @Test
    void testBatcherStopDoesNotWaitForCompletion() {
        loadFifteenAuthorsIntoMarkLogic();

        RowBatcherSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );
        List<SourceRecord> newSourceRecords = new Vector<>();
        RowBatcher<?> rowBatcher = task.getNewRowBatcher(newSourceRecords);

        // Register our own success listener to look for any onSuccess events
        // and set a variable tracking onSuccess events.
        AtomicReference<Boolean> onSuccessCalled = new AtomicReference<>(false);
        rowBatcher.onSuccess(event -> onSuccessCalled.set(true));

        Map<String, Object> parsedConfig = new HashMap<>();

        // Start a new thread that can be paused before polling
        // to verify that task.stop() prevents any new polling.
        Thread t1 = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            QueryHandler queryHandler = QueryHandler.newQueryHandler(getDatabaseClient(), parsedConfig);
            task.performPoll(queryHandler, newSourceRecords);
        });
        t1.start();

        task.stop();
        Assertions.assertFalse(onSuccessCalled.get(),
            "RowBatcherSourceTask should have stopped before any onSuccess events");
        Assertions.assertEquals(0, newSourceRecords.size());
    }
}
