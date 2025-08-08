/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import com.marklogic.junit5.spring.SimpleTestConfig;
import com.marklogic.kafka.connect.AbstractIntegrationTest;
import org.apache.kafka.connect.sink.SinkRecord;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.marklogic.kafka.connect.sink.MarkLogicSinkConfig.*;

/**
 * Base class for any test that wishes to connect to the
 * "kafka-test-test-content" app server on port 8019.
 * AbstractSpringMarkLogicTest assumes it can find
 * mlHost/mlTestRestPort/mlUsername/mlPassword properties in
 * gradle.properties and gradle-local.properties. It uses those to construct a
 * DatabaseClient which can be fetched
 * via getDatabaseClient().
 */
public abstract class AbstractIntegrationSinkTest extends AbstractIntegrationTest {

    // Declared by AbstractSpringMarkLogicTest
    @Autowired
    SimpleTestConfig testConfig;

    private Map<String, Object> taskConfig = new HashMap<>();

    /**
     * @param configParamNamesAndValues - Configuration values that need to be set
     *                                  for the test.
     * @return a MarkLogicSinkTask based on the default connection config and any
     *         optional config params provided by
     *         the caller
     */
    protected AbstractSinkTask startSinkTask(String... configParamNamesAndValues) {
        return startSinkTask(null, configParamNamesAndValues);
    }

    protected AbstractSinkTask startSinkTask(BiConsumer<SinkRecord, Throwable> errorReporterMethod,
            String... configParamNamesAndValues) {
        Map<String, String> config = newMarkLogicConfig(testConfig);
        config.put(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS, "rest-reader,read,rest-writer,update");
        for (int i = 0; i < configParamNamesAndValues.length; i += 2) {
            config.put(configParamNamesAndValues[i], configParamNamesAndValues[i + 1]);
        }
        taskConfig.putAll(config);
        if (taskConfig.containsKey(DMSDK_INCLUDE_KAFKA_METADATA)) {
            taskConfig.put(DMSDK_INCLUDE_KAFKA_METADATA,
                    Boolean.valueOf((String) taskConfig.get(DMSDK_INCLUDE_KAFKA_METADATA)));
        }
        if (taskConfig.containsKey(DOCUMENT_COLLECTIONS_ADD_TOPIC)) {
            taskConfig.put(DOCUMENT_COLLECTIONS_ADD_TOPIC,
                    Boolean.valueOf((String) taskConfig.get(DOCUMENT_COLLECTIONS_ADD_TOPIC)));
        }

        MarkLogicSinkConnector connector = new MarkLogicSinkConnector();
        connector.start(config);
        AbstractSinkTask task;
        try {
            task = (AbstractSinkTask) connector.taskClass().getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        if (errorReporterMethod != null) {
            task.setErrorReporterMethod(errorReporterMethod);
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

    protected Map<String, Object> getTaskConfig() {
        return taskConfig;
    }
}
