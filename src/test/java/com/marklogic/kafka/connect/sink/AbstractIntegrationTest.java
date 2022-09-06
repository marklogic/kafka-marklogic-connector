package com.marklogic.kafka.connect.sink;

import com.marklogic.junit5.spring.AbstractSpringMarkLogicTest;
import com.marklogic.junit5.spring.SimpleTestConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for any test that wishes to connect to the "kafka-test-test-content" app server on port 8019.
 * AbstractSpringMarkLogicTest assumes it can find mlHost/mlTestRestPort/mlUsername/mlPassword properties in
 * gradle.properties and gradle-local.properties. It uses those to construct a DatabaseClient which can be fetched
 * via getDatabaseClient().
 */
public class AbstractIntegrationTest extends AbstractSpringMarkLogicTest {

    // Declared by AbstractSpringMarkLogicTest
    @Autowired
    SimpleTestConfig testConfig;

    /**
     * @return a config map containing connection values based on the test application configuration
     */
    protected Map<String, String> newSinkConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("ml.connection.host", testConfig.getHost());
        config.put("ml.connection.port", testConfig.getRestPort() + "");
        config.put("ml.connection.securityContextType", "DIGEST");
        config.put("ml.connection.username", testConfig.getUsername());
        config.put("ml.connection.password", testConfig.getPassword());
        return config;
    }

    /**
     * @param configParamNamesAndValues
     * @return a MarkLogicSinkTask based on the default connection config and any optional config params provided by
     * the caller
     */
    protected AbstractSinkTask startSinkTask(String... configParamNamesAndValues) {
        Map<String, String> config = newSinkConfig();
        for (int i = 0; i < configParamNamesAndValues.length; i += 2) {
            config.put(configParamNamesAndValues[i], configParamNamesAndValues[i + 1]);
        }
        MarkLogicSinkConnector connector = new MarkLogicSinkConnector();
        connector.start(config);
        AbstractSinkTask task;
        try {
            task = (AbstractSinkTask) connector.taskClass().newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        task.start(config);
        return task;
    }

    protected SinkRecord newSinkRecord(String content) {
        String topic = "topic-name-doesnt-matter";
        return new SinkRecord(topic, 1, null, null, null, content, 0);
    }

    protected void putAndFlushRecords(AbstractSinkTask task, SinkRecord... records) {
        task.put(Arrays.asList(records));
        task.flush(new HashMap<>());
    }
}
