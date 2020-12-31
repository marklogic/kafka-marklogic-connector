package com.marklogic.kafka.connect.sink;

import com.marklogic.client.ext.DatabaseClientConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BuildSuccessListenerTest {

	MarkLogicSinkTask task = new MarkLogicSinkTask();
	Map<String, Object> config = new HashMap<>();

	@Test
	void multipleSteps() {
		config.put(MarkLogicSinkConfig.DATAHUB_FLOW_STEPS, "1,2");
		RunFlowWriteBatchListener listener = task.buildSuccessListener("myFlow", config, new DatabaseClientConfig());

		assertEquals("myFlow", listener.getFlowName());
		List<String> steps = listener.getSteps();
		assertEquals(2, steps.size());
		assertEquals("1", steps.get(0));
		assertEquals("2", steps.get(1));
	}

	@Test
	void emptySteps() {
		config.put(MarkLogicSinkConfig.DATAHUB_FLOW_STEPS, " ");
		RunFlowWriteBatchListener listener = task.buildSuccessListener("myFlow", config, new DatabaseClientConfig());
		assertNull(listener.getSteps());
	}

	@Test
	void noSteps() {
		RunFlowWriteBatchListener listener = task.buildSuccessListener("myFlow", config, new DatabaseClientConfig());
		assertNull(listener.getSteps());
	}

}
