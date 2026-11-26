/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import com.marklogic.client.datamovement.WriteBatcher;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verifies the task lifecycle paths that Kafka Connect drives - stopping a started task, and resolving the errant
 * record reporter from the context that Kafka provides.
 */
class SinkTaskLifecycleTest extends AbstractIntegrationSinkTest {

    private static final String COLLECTION = "lifecycle-test";

    @Test
    void writeBatcherTaskStopsAfterWriting() {
        AbstractSinkTask task = startSinkTask(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, COLLECTION);

        putAndFlushRecords(task, newSinkRecord("{\"hello\":\"world\"}"));
        assertCollectionSize("The record should be written before the task is stopped", COLLECTION, 1);

        assertDoesNotThrow(task::stop, "stop must stop the DMSDK job and release the DatabaseClient");
        assertEquals(true, ((WriteBatcherSinkTask) task).getWriteBatcher().isStopped(),
             "Stopping the task must stop the DMSDK job");
    }

    @Test
    void bulkDataServicesTaskStopsAfterWriting() {
        AbstractSinkTask task = startSinkTask(
            MarkLogicSinkConfig.BULK_DS_ENDPOINT_URI, "/example/bulk-endpoint.sjs"
        );

        task.put(List.of(newSinkRecord("<anything/>")));
        assertCollectionSize("The record must still be queued before shutdown", "bulk-ds-test", 0);

        assertDoesNotThrow(task::stop, "stop must flush the BulkInputCaller and release the DatabaseClient");
        assertCollectionSize("Stopping must flush the queued record", "bulk-ds-test", 1);
        assertEquals("<anything/>",
             readJsonDocument(getUrisInCollection("bulk-ds-test", 1).get(0)).get("content").asText());
    }

    @Test
    void configuresTheWriteBatcherFromTheDmsdkOptions() {
        AbstractSinkTask task = startSinkTask(
            MarkLogicSinkConfig.DMSDK_TRANSFORM, "exampleTransform",
            MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS, "param1;value1",
            MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS_DELIMITER, ";",
            MarkLogicSinkConfig.DMSDK_BATCH_SIZE, "50",
            MarkLogicSinkConfig.DMSDK_THREAD_COUNT, "2"
        );

        try {
            WriteBatcher writeBatcher = ((WriteBatcherSinkTask) task).getWriteBatcher();
            assertEquals(50, writeBatcher.getBatchSize());
            assertEquals(2, writeBatcher.getThreadCount());
            assertEquals("exampleTransform", writeBatcher.getTransform().getName(),
                "The configured transform must be applied to every batch");
        } finally {
            task.stop();
        }
    }

    @Test
    void usesTheErrantRecordReporterProvidedByTheContext() {
        List<SinkRecord> reported = new ArrayList<>();
        ErrantRecordReporter reporter = (ErrantRecordReporter) Proxy.newProxyInstance(
            ErrantRecordReporter.class.getClassLoader(), new Class<?>[]{ErrantRecordReporter.class},
            (proxy, method, args) -> {
                reported.add((SinkRecord) args[0]);
                return CompletableFuture.completedFuture(null);
            });

        AbstractSinkTask task = startTaskWithContext(() -> reporter);
        try {
            SinkRecord record = newSinkRecord("{\"a\":1}");
            task.errorReporterMethod.accept(record, new IllegalStateException("simulated failure"));

            assertEquals(List.of(record), reported,
                "Failures must reach the reporter so Kafka can route them to the dead letter queue");
        } finally {
            task.stop();
        }
    }

    @Test
    void fallsBackToANoOpReporterWhenTheContextHasNoReporter() {
        AbstractSinkTask task = startTaskWithContext(() -> null);
        try {
            assertNotNull(task.errorReporterMethod);
            assertDoesNotThrow(() -> task.errorReporterMethod.accept(newSinkRecord("{\"a\":1}"),
                new IllegalStateException("simulated failure")));
        } finally {
            task.stop();
        }
    }

    @Test
    void fallsBackToANoOpReporterOnKafkaVersionsBefore26() {
        AbstractSinkTask task = startTaskWithContext(() -> {
            throw new NoSuchMethodError("simulating a Connect runtime older than 2.6");
        });
        try {
            assertDoesNotThrow(() -> task.errorReporterMethod.accept(newSinkRecord("{\"a\":1}"),
                new IllegalStateException("simulated failure")));
        } finally {
            task.stop();
        }
    }

    private AbstractSinkTask startTaskWithContext(Supplier<ErrantRecordReporter> reporterSupplier) {
        SinkTaskContext context = (SinkTaskContext) Proxy.newProxyInstance(
            SinkTaskContext.class.getClassLoader(), new Class<?>[]{SinkTaskContext.class},
            (proxy, method, args) -> "errantRecordReporter".equals(method.getName()) ? reporterSupplier.get() : null);

        Map<String, String> config = newMarkLogicConfig(testConfig);
        config.put(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS, "rest-reader,read,rest-writer,update");
        config.put(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, COLLECTION);

        WriteBatcherSinkTask task = new WriteBatcherSinkTask();
        task.initialize(context);
        task.start(config);
        return task;
    }
}
