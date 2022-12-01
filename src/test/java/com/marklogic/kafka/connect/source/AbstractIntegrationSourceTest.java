package com.marklogic.kafka.connect.source;

import com.marklogic.junit5.spring.SimpleTestConfig;
import com.marklogic.kafka.connect.AbstractIntegrationTest;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

/**
 * Base class for any test that wishes to connect to the "kafka-test-test-content" app server on port 8019.
 * AbstractSpringMarkLogicTest assumes it can find mlHost/mlTestRestPort/mlUsername/mlPassword properties in
 * gradle.properties and gradle-local.properties. It uses those to construct a DatabaseClient which can be fetched
 * via getDatabaseClient().
 */
public class AbstractIntegrationSourceTest extends AbstractIntegrationTest {

    // Declared by AbstractSpringMarkLogicTest
    @Autowired
    protected SimpleTestConfig testConfig;

    protected final String AUTHORS_OPTIC_DSL = "op.fromView(\"Medical\", \"Authors\")";
    protected final String AUTHORS_TOPIC = "Authors";

    /**
     * @param configParamNamesAndValues - Configuration values that need to be set for the test.
     * @return a MarkLogicSourceTask based on the default connection config and any optional config params provided by
     * the caller
     */
    protected RowBatcherSourceTask startSourceTask(String... configParamNamesAndValues) {
        Map<String, String> config = newMarkLogicConfig(testConfig);
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
