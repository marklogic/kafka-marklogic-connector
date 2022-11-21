package com.marklogic.kafka.connect.sink;

import com.marklogic.junit5.spring.SimpleTestConfig;
import com.marklogic.kafka.connect.AbstractIntegrationTest;
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
public class AbstractIntegrationSinkTest extends AbstractIntegrationTest {

    // Declared by AbstractSpringMarkLogicTest
    @Autowired
    SimpleTestConfig testConfig;

    private final static long DEFAULT_RETRY_SLEEP_TIME = 250;
    private final static int DEFAULT_RETRY_ATTEMPTS = 10;

    /**
     * @param configParamNamesAndValues - Configuration values that need to be set for the test.
     * @return a MarkLogicSinkTask based on the default connection config and any optional config params provided by
     * the caller
     */
    protected AbstractSinkTask startSinkTask(String... configParamNamesAndValues) {
        Map<String, String> config = newMarkLogicConfig(testConfig);
        config.put(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS, "rest-reader,read,rest-writer,update");

        for (int i = 0; i < configParamNamesAndValues.length; i += 2) {
            config.put(configParamNamesAndValues[i], configParamNamesAndValues[i + 1]);
        }
        MarkLogicSinkConnector connector = new MarkLogicSinkConnector();
        connector.start(config);
        AbstractSinkTask task;
        try {
            task = (AbstractSinkTask) connector.taskClass().getDeclaredConstructor().newInstance();
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

    protected final void retryIfNotSuccessful(Runnable r) {
        retryIfNotSuccessful(r, DEFAULT_RETRY_SLEEP_TIME, DEFAULT_RETRY_ATTEMPTS);
    }

    protected final void retryIfNotSuccessful(Runnable r, long sleepTime, int attempts) {
        for (int i = 1; i <= attempts; i++) {
            logger.info("Trying assertion, attempt " + i + " out of " + attempts);
            try {
                r.run();
                return;
            } catch (Throwable ex) {
                if (i == attempts) {
                    throw ex;
                }
                logger.info("Assertion failed: " + ex.getMessage() + "; will sleep for " + sleepTime + " ms and try again");
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    // Ignore, not expected during a test
                }
            }
        }
    }
}
