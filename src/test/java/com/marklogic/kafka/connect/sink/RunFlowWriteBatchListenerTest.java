package com.marklogic.kafka.connect.sink;

import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.datamovement.impl.WriteBatchImpl;
import com.marklogic.client.datamovement.impl.WriteEventImpl;
import com.marklogic.hub.flow.FlowInputs;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RunFlowWriteBatchListenerTest {

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

}
