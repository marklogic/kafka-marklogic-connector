package com.marklogic.kafka.connect.source;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CreateTasksTest extends AbstractIntegrationSourceTest {

    @Test
    void taskCountGreaterThanOne() {
        Map<String, String> config = newMarkLogicConfig(testConfig);
        config.put(MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL);
        MarkLogicSourceConnector connector = new MarkLogicSourceConnector();
        connector.start(config);

        List configs = connector.taskConfigs(2);
        assertEquals(1, configs.size(),
            "For the 1.8.0 release, no matter what the users sets the Kafka tasks.max property to, we are only " +
                "supporting one task to prevent the user from a configuration that we do not believe will ever be " +
                "valid. That is - 2+ tasks with the same Optic query and config will produce 2+ copies of every row, " +
                "and it will also encounter race conditions if the constraint column config is stored in MarkLogic. " +
                "This restriction can be relaxed in the future if users identify valid scenarios for having 2+ tasks.");
    }
}
