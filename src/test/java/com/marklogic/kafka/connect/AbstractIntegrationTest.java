package com.marklogic.kafka.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.junit5.spring.AbstractSpringMarkLogicTest;
import com.marklogic.junit5.spring.SimpleTestConfig;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;

import java.util.HashMap;
import java.util.Map;

public class AbstractIntegrationTest extends AbstractSpringMarkLogicTest {

    protected final static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * @return a config map containing connection values based on the test application configuration
     */
    protected Map<String, String> newMarkLogicConfig(SimpleTestConfig testConfig) {
        Map<String, String> config = new HashMap<>();
        config.put(MarkLogicSinkConfig.CONNECTION_HOST, testConfig.getHost());
        config.put(MarkLogicSinkConfig.CONNECTION_PORT, testConfig.getRestPort() + "");
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "DIGEST");

        // Default to a "bare minimum" user, which is defined in src/test/ml-config
        config.put(MarkLogicSinkConfig.CONNECTION_USERNAME, "kafka-test-user");
        config.put(MarkLogicSinkConfig.CONNECTION_PASSWORD, "kafkatest");

        return config;
    }

    /**
     * Convenience for getting a JSON object from a String of JSON without having to worry
     * about the annoying checked exception.
     *
     * @param json
     * @return
     */
    protected ObjectNode readJsonObject(String json) {
        try {
            return (ObjectNode) objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to read JSON: " + e.getMessage(), e);
        }
    }
}
