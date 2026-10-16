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
import java.util.Collections;
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
        assertEquals(RunFlowWriteBatchListener.SOURCE_QUERY, options.get("sourceQuery"),
            "The source query must be a constant expression that reads the URIs from an external variable, so that no " +
                "record-derived content is interpolated into the JavaScript that DHF evaluates");
        assertEquals(Arrays.asList("uri1", "uri2", "uri3"), options.get("uris"),
            "The URIs are passed as data via the 'uris' option instead of being inlined into the source query");
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
    void uriContainingJavaScriptBreakoutPayload() {
        RunFlowWriteBatchListener listener = new RunFlowWriteBatchListener("myFlow", null, null);

        MockWriteBatcher mockWriteBatcher = new MockWriteBatcher();
        mockWriteBatcher.jobId = "job123";

        String maliciousUri = "x']); xdmp.log('INJECTED'); cts.documentQuery(['y";
        String complexUri = "/data/file with \"quotes\" and 'apostrophes' and ; semicolons.json";

        WriteBatchImpl batch = new WriteBatchImpl()
            .withJobBatchNumber(1)
            .withBatcher(mockWriteBatcher)
            .withItems(new WriteEvent[]{
                new WriteEventImpl().withTargetUri(maliciousUri),
                new WriteEventImpl().withTargetUri(complexUri)
            });

        Map<String, Object> options = listener.buildFlowInputs(batch).getOptions();

        assertEquals(RunFlowWriteBatchListener.SOURCE_QUERY, options.get("sourceQuery"),
            "A URI that attempts to break out of a JavaScript string literal must not affect the source query");
        assertEquals(Arrays.asList(maliciousUri, complexUri), options.get("uris"),
            "The URIs are passed verbatim as data, so no escaping of quotes or semicolons is required");
    }

    @Test
    void emptyBatch() {
        RunFlowWriteBatchListener listener = new RunFlowWriteBatchListener("myFlow", null, null);

        MockWriteBatcher mockWriteBatcher = new MockWriteBatcher();
        mockWriteBatcher.jobId = "job123";

        WriteBatchImpl batch = new WriteBatchImpl()
            .withJobBatchNumber(1)
            .withBatcher(mockWriteBatcher)
            .withItems(new WriteEvent[]{});

        Map<String, Object> options = listener.buildFlowInputs(batch).getOptions();

        assertEquals(RunFlowWriteBatchListener.SOURCE_QUERY, options.get("sourceQuery"));
        assertEquals(Collections.emptyList(), options.get("uris"));
    }
}
