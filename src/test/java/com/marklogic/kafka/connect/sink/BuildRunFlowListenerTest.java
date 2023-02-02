/*
 * Copyright (c) 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.kafka.connect.sink;

import com.marklogic.client.ext.DatabaseClientConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BuildRunFlowListenerTest {

    WriteBatcherSinkTask task = new WriteBatcherSinkTask();
    Map<String, Object> config = new HashMap<>();

    @Test
    void multipleSteps() {
        config.put(MarkLogicSinkConfig.DATAHUB_FLOW_STEPS, "1,2");
        RunFlowWriteBatchListener listener = task.buildRunFlowListener("myFlow", config, new DatabaseClientConfig());

        assertEquals("myFlow", listener.getFlowName());
        List<String> steps = listener.getSteps();
        assertEquals(2, steps.size());
        assertEquals("1", steps.get(0));
        assertEquals("2", steps.get(1));
    }

    @Test
    void emptySteps() {
        config.put(MarkLogicSinkConfig.DATAHUB_FLOW_STEPS, " ");
        RunFlowWriteBatchListener listener = task.buildRunFlowListener("myFlow", config, new DatabaseClientConfig());
        assertNull(listener.getSteps());
    }

    @Test
    void noSteps() {
        RunFlowWriteBatchListener listener = task.buildRunFlowListener("myFlow", config, new DatabaseClientConfig());
        assertNull(listener.getSteps());
    }

}
