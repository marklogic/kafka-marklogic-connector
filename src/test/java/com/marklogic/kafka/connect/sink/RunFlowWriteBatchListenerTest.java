/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.datamovement.impl.WriteBatchImpl;
import com.marklogic.client.datamovement.impl.WriteEventImpl;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.hub.flow.FlowInputs;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RunFlowWriteBatchListenerTest {

    @Test
    void buildFlowInputs() {
        RunFlowWriteBatchListener listener = new RunFlowWriteBatchListener("myFlow",
            Arrays.asList("1", "2", "3"), null);

        MockWriteBatcher mockWriteBatcher = new MockWriteBatcher();
        mockWriteBatcher.jobId = "job123";

        WriteBatchImpl batch = new WriteBatchImpl()
            .withJobBatchNumber(100)
            .withBatcher(mockWriteBatcher)
            .withItems(new WriteEvent[]{
                new WriteEventImpl().withTargetUri("uri1"),
                new WriteEventImpl().withTargetUri("uri2"),
                new WriteEventImpl().withTargetUri("uri3")
            });

        final FlowInputs inputs = listener.buildFlowInputs(batch);

        assertEquals("myFlow", inputs.getFlowName());
        assertEquals(3, inputs.getSteps().size());
        assertEquals("1", inputs.getSteps().get(0));
        assertEquals("2", inputs.getSteps().get(1));
        assertEquals("3", inputs.getSteps().get(2));
        assertEquals("job123-100", inputs.getJobId());

        Map<String, Object> options = inputs.getOptions();
        assertEquals("cts.documentQuery(['uri1','uri2','uri3'])", options.get("sourceQuery"),
            "The source query is expected to constrain on each of the documents in the WriteBatch");
    }

    @Test
    void buildFlowInputsWithoutSteps() {
        DatabaseClientConfig databaseClientConfig = new DatabaseClientConfig("somehost", 8000);
        RunFlowWriteBatchListener listener = new RunFlowWriteBatchListener("myFlow", null, databaseClientConfig);

        MockWriteBatcher mockWriteBatcher = new MockWriteBatcher();
        mockWriteBatcher.jobId = "job456";
        WriteBatchImpl batch = new WriteBatchImpl()
            .withJobBatchNumber(7)
            .withBatcher(mockWriteBatcher)
            .withItems(new WriteEvent[]{new WriteEventImpl().withTargetUri("/one.json")});

        FlowInputs inputs = listener.buildFlowInputs(batch);

        assertEquals("myFlow", inputs.getFlowName());
        assertNull(inputs.getSteps(), "All steps in the flow run when no steps are configured");
        assertEquals("job456-7", inputs.getJobId());
        assertEquals("myFlow", listener.getFlowName());
        assertNull(listener.getSteps());
        assertEquals(databaseClientConfig, listener.getDatabaseClientConfig(),
            "DHF needs the config because it cannot reuse the DatabaseClient that Kafka constructs");
    }

    @Test
    void buildSourceQueryForEmptyAndSingleItemBatches() {
        RunFlowWriteBatchListener listener = new RunFlowWriteBatchListener("myFlow", null, null);

        WriteBatchImpl emptyBatch = new WriteBatchImpl().withItems(new WriteEvent[]{});
        assertEquals("cts.documentQuery([])", listener.buildSourceQuery(emptyBatch));

        WriteBatchImpl oneItemBatch = new WriteBatchImpl().withItems(new WriteEvent[]{
            new WriteEventImpl().withTargetUri("/one.json")
        });
        assertEquals("cts.documentQuery(['/one.json'])", listener.buildSourceQuery(oneItemBatch));
    }

}
