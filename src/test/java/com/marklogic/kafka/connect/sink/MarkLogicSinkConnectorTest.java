/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MarkLogicSinkConnectorTest {

    @Test
    void usesWriteBatcherTaskWhenBulkEndpointIsNotConfigured() {
        MarkLogicSinkConnector connector = new MarkLogicSinkConnector();
        Map<String, String> config = new HashMap<>();
        connector.start(config);

        assertNotNull(connector.config());
        assertEquals(WriteBatcherSinkTask.class, connector.taskClass());
        assertEquals(0, connector.taskConfigs(0).size());
        assertEquals(2, connector.taskConfigs(2).size());
        connector.version();
        connector.stop();
    }

    @Test
    void usesBulkDataServicesTaskWhenEndpointIsConfigured() {
        MarkLogicSinkConnector connector = new MarkLogicSinkConnector();
        Map<String, String> config = new HashMap<>();
        config.put(MarkLogicSinkConfig.BULK_DS_ENDPOINT_URI, "/example/endpoint.sjs");
        connector.start(config);

        assertEquals(BulkDataServicesSinkTask.class, connector.taskClass());
        assertEquals(1, connector.taskConfigs(1).size());
        connector.stop();
    }

    @Test
    void sinkConfigCanBeConstructedDirectly() {
        Map<String, String> config = new HashMap<>();
        config.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        config.put(MarkLogicSinkConfig.CONNECTION_PORT, "8000");
        config.put(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, "one,two");

        MarkLogicSinkConfig sinkConfig = new MarkLogicSinkConfig(config);

        assertEquals("localhost", sinkConfig.getString(MarkLogicSinkConfig.CONNECTION_HOST));
        assertEquals(8000, sinkConfig.getInt(MarkLogicSinkConfig.CONNECTION_PORT));
        assertEquals("one,two", sinkConfig.getString(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS));
    }
}
