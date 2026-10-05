/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.source;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.kafka.connect.MarkLogicConnectorException;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SourceFactoryTest {

    @Test
    void planInvokerDefaultsToJsonForMissingOrBlankFormat() {
        assertInstanceOf(JsonPlanInvoker.class, PlanInvoker.newPlanInvoker(null, new HashMap<>()));

        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.OUTPUT_FORMAT, "  ");
        assertInstanceOf(JsonPlanInvoker.class, PlanInvoker.newPlanInvoker(null, config));
    }

    @Test
    void planInvokerSelectsConfiguredFormats() {
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.OUTPUT_FORMAT, "CSV");
        assertInstanceOf(CsvPlanInvoker.class, PlanInvoker.newPlanInvoker(null, config));

        config.put(MarkLogicSourceConfig.OUTPUT_FORMAT, "XML");
        assertInstanceOf(XmlPlanInvoker.class, PlanInvoker.newPlanInvoker(null, config));
    }

    @Test
    void planInvokerRejectsUnknownFormat() {
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.OUTPUT_FORMAT, "YAML");
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> PlanInvoker.newPlanInvoker(null, config));
        assertTrue(error.getMessage().contains("YAML"));
    }

    @Test
    void queryHandlerRequiresExactlyOneQuery() {
        ConfigException missingQueryError = assertThrows(ConfigException.class,
            () -> QueryHandler.newQueryHandler(null, new HashMap<>()));
        assertTrue(missingQueryError.getMessage().contains("Either a DSL Optic query"));

        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.DSL_QUERY, "op.fromView('schema', 'view')");
        config.put(MarkLogicSourceConfig.SERIALIZED_QUERY, "{}");
        ConfigException bothQueriesError = assertThrows(ConfigException.class,
            () -> QueryHandler.newQueryHandler(null, config));
        assertTrue(bothQueriesError.getMessage().contains("but not both"));
    }

    @Test
    void serializedQueryHandlerRejectsMalformedJson() {
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.SERIALIZED_QUERY, "{malformed");

        MarkLogicConnectorException error = assertThrows(MarkLogicConnectorException.class,
            () -> new SerializedQueryHandler(null, config));
        assertTrue(error.getMessage().startsWith("Unable to read serialized query; cause:"));
    }

    @Test
    void sourceConnectorExposesConfigurationAndTaskMetadata() {
        MarkLogicSourceConnector connector = new MarkLogicSourceConnector();
        Map<String, String> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.DSL_QUERY, "op.fromView('schema', 'view')");

        connector.start(config);

        assertEquals(MarkLogicSourceConfig.CONFIG_DEF, connector.config());
        assertEquals(RowManagerSourceTask.class, connector.taskClass());
        assertEquals(config, connector.taskConfigs(1).get(0));
        assertEquals(MarkLogicSourceConnector.MARKLOGIC_SOURCE_CONNECTOR_VERSION, connector.version());
        connector.stop();
    }

    @Test
    void sourceConfigCanBeConstructedDirectly() {
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.CONNECTION_HOST, "localhost");
        config.put(MarkLogicSourceConfig.CONNECTION_PORT, "8000");
        config.put(MarkLogicSourceConfig.TOPIC, "topic1");
        config.put(MarkLogicSourceConfig.DSL_QUERY, "op.fromView('schema', 'view')");

        MarkLogicSourceConfig sourceConfig = new MarkLogicSourceConfig(config);

        assertEquals("localhost", sourceConfig.getString(MarkLogicSourceConfig.CONNECTION_HOST));
        assertEquals(8000, sourceConfig.getInt(MarkLogicSourceConfig.CONNECTION_PORT));
        assertEquals("topic1", sourceConfig.getString(MarkLogicSourceConfig.TOPIC));
    }

    @Test
    void documentWriteOperationBuilderRejectsMissingContent() {
        RecordContent recordContent = new RecordContent();
        recordContent.setAdditionalMetadata(new DocumentMetadataHandle());

        NullPointerException error = assertThrows(NullPointerException.class,
            () -> new DocumentWriteOperationBuilder().build(recordContent));
        assertEquals("'content' must not be null", error.getMessage());
    }

    @Test
    void documentWriteOperationBuilderAppliesConfiguredTypeAndUriParts() {
        RecordContent recordContent = new RecordContent();
        recordContent.setContent(new com.marklogic.client.io.StringHandle("content"));
        recordContent.setAdditionalMetadata(new DocumentMetadataHandle());
        recordContent.setId("record");

        DocumentWriteOperationBuilder builder = new DocumentWriteOperationBuilder()
            .withUriPrefix("/prefix/")
            .withUriSuffix(".json")
            .withOperationType(DocumentWriteOperation.OperationType.DOCUMENT_WRITE);

        DocumentWriteOperation operation = builder.build(recordContent);
        assertEquals("/prefix/record.json", operation.getUri());
        assertEquals(DocumentWriteOperation.OperationType.DOCUMENT_WRITE, operation.getOperationType());
    }
}
