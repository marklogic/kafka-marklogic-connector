/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import org.apache.kafka.connect.runtime.ConnectorConfig;

import java.util.Properties;
import java.util.UUID;

public class MarkLogicSinkConnectorConfigBuilder {

    private String topic = String.format("topic-%s", UUID.randomUUID().toString());

    private String key = String.format("key-%s", UUID.randomUUID().toString());

    private final Properties connectorProps = new Properties();

    public MarkLogicSinkConnectorConfigBuilder withTopic(final String topic) {
        this.topic = topic;
        return this;
    }

    public MarkLogicSinkConnectorConfigBuilder withKey(final String key) {
        this.key = key;
        return this;
    }

    public <T> MarkLogicSinkConnectorConfigBuilder with(final String propertyName, final T value) {
        connectorProps.put(propertyName, value);
        return this;
    }

    public <T> MarkLogicSinkConnectorConfigBuilder withAll(final Properties connectorProps) {
        this.connectorProps.putAll(connectorProps);
        return this;
    }

    private <T> void ifNonExisting(final String propertyName, final T value) {
        if (connectorProps.get(propertyName) != null) return;
        connectorProps.put(propertyName, value);
    }

    public Properties build() {

        ifNonExisting(ConnectorConfig.NAME_CONFIG, "marklogic-sink");
        ifNonExisting(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "MarkLogicSinkConnector");
        ifNonExisting(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        ifNonExisting("topics", topic);
        ifNonExisting("key", key);

        ifNonExisting(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        ifNonExisting(MarkLogicSinkConfig.CONNECTION_PORT, "8000");
        ifNonExisting(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "DIGEST");
        ifNonExisting(MarkLogicSinkConfig.CONNECTION_USERNAME, "admin");
        ifNonExisting(MarkLogicSinkConfig.CONNECTION_PASSWORD, "admin");
        ifNonExisting(MarkLogicSinkConfig.DOCUMENT_FORMAT, "JSON");
        ifNonExisting(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, "kafka-data");
        ifNonExisting(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS, "rest-reader,read,rest-writer,update");
        ifNonExisting(MarkLogicSinkConfig.DOCUMENT_URI_PREFIX, "/kafka-data/");
        ifNonExisting(MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX, ".json");
        ifNonExisting(MarkLogicSinkConfig.DMSDK_BATCH_SIZE, "100");
        ifNonExisting(MarkLogicSinkConfig.DMSDK_THREAD_COUNT, "8");
        ifNonExisting("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        ifNonExisting("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        ifNonExisting("errors.log.enable", true);

        final Properties copyOfConnectorProps = new Properties();
        copyOfConnectorProps.putAll(connectorProps);

        return copyOfConnectorProps;
    }

    public static MarkLogicSinkConnectorConfigBuilder create() {
        return new MarkLogicSinkConnectorConfigBuilder();
    }
}
