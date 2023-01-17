package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.ServerTransform;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BuildServerTransformTest {

    private WriteBatcherSinkTask task = new WriteBatcherSinkTask();
    private Map<String, Object> config = new HashMap<>();

    @Test
    void noTransform() {
        assertFalse(task.buildServerTransform(config).isPresent());
    }

    @Test
    void noParams() {
        config.put(MarkLogicSinkConfig.DMSDK_TRANSFORM, "noParams");
        ServerTransform t = task.buildServerTransform(config).get();
        assertEquals("noParams", t.getName());
        assertTrue(t.keySet().isEmpty());
    }

    @Test
    void oneParam() {
        config.put(MarkLogicSinkConfig.DMSDK_TRANSFORM, "oneParam");
        config.put(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS, "param1,value1");
        config.put(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS_DELIMITER, ",");
        ServerTransform t = task.buildServerTransform(config).get();
        assertEquals(1, t.keySet().size());
        assertEquals("value1", t.get("param1").get(0));
    }

    @Test
    void twoParamsWithCustomDelimiter() {
        config.put(MarkLogicSinkConfig.DMSDK_TRANSFORM, "twoParams");
        config.put(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS, "param1;value1;param2;value2");
        config.put(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS_DELIMITER, ";");
        ServerTransform t = task.buildServerTransform(config).get();
        assertEquals(2, t.keySet().size());
        assertEquals("value1", t.get("param1").get(0));
        assertEquals("value2", t.get("param2").get(0));
    }

    @Test
    void malformedParams() {
        config.put(MarkLogicSinkConfig.DMSDK_TRANSFORM, "malformedParams");
        config.put(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS, "param1,value1,param2");
        config.put(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS_DELIMITER, ",");
        try {
            task.buildServerTransform(config);
            fail("The call should have failed because the params property does not have an even number of parameter " +
                "names and values");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().startsWith("The value of the ml.dmsdk.transformParams property"));
        }
    }
}
