package com.marklogic.kafka.connect.source;

import com.marklogic.junit5.spring.AbstractSpringMarkLogicTest;
import com.marklogic.junit5.spring.SimpleTestConfig;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for any test that wishes to connect to the "kafka-test-test-content" app server on port 8019.
 * AbstractSpringMarkLogicTest assumes it can find mlHost/mlTestRestPort/mlUsername/mlPassword properties in
 * gradle.properties and gradle-local.properties. It uses those to construct a DatabaseClient which can be fetched
 * via getDatabaseClient().
 */
public class AbstractIntegrationSourceTest extends AbstractSpringMarkLogicTest {

    // Declared by AbstractSpringMarkLogicTest
    @Autowired
    protected SimpleTestConfig testConfig;

    /**
     * @return a config map containing connection values based on the test application configuration
     */
    // This isn't called yet, but it will be...
    private Map<String, String> newSourceConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.CONNECTION_HOST, testConfig.getHost());
        config.put(MarkLogicSourceConfig.CONNECTION_PORT, testConfig.getRestPort() + "");
        config.put(MarkLogicSourceConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "DIGEST");
        config.put(MarkLogicSourceConfig.CONNECTION_USERNAME, "kafka-test-user");
        config.put(MarkLogicSourceConfig.CONNECTION_PASSWORD, "kafkatest");
        return config;
    }

    /**
     * @param configParamNamesAndValues
     * @return a MarkLogicSourceTask based on the default connection config and any optional config params provided by
     * the caller
     */
    // This isn't called yet, but it will be...
    protected RowBatcherSourceTask startSourceTask(String... configParamNamesAndValues) {
        Map<String, String> config = newSourceConfig();
        for (int i = 0; i < configParamNamesAndValues.length; i += 2) {
            config.put(configParamNamesAndValues[i], configParamNamesAndValues[i + 1]);
        }
        MarkLogicSourceConnector connector = new MarkLogicSourceConnector();
        connector.start(config);
        RowBatcherSourceTask task;
        try {
            task = (RowBatcherSourceTask) connector.taskClass().getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        task.start(config);
        return task;
    }
}
