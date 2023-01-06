package com.marklogic.kafka.connect.source;

import org.apache.kafka.connect.runtime.ConnectorConfig;

import java.util.Properties;
import java.util.UUID;

public class MarkLogicSourceConnectorConfigBuilder {

    private String topic = String.format("topic-%s", UUID.randomUUID());

    private String dsl;

    private final Properties connectorProps = new Properties();

    public MarkLogicSourceConnectorConfigBuilder withTopic(final String topic) {
        this.topic = topic;
        return this;
    }

    public MarkLogicSourceConnectorConfigBuilder withDsl(final String dsl) {
        this.dsl = dsl;
        return this;
    }

    public <T> MarkLogicSourceConnectorConfigBuilder with(final String propertyName, final T value) {
        connectorProps.put(propertyName, value);
        return this;
    }

    private <T> void ifNonExisting(final String propertyName, final T value) {
        if (connectorProps.get(propertyName) != null) return;
        connectorProps.put(propertyName, value);
    }

    public Properties build() {

        ifNonExisting(ConnectorConfig.NAME_CONFIG, "marklogic-source");
        ifNonExisting(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "MarkLogicSourceConnector");
        ifNonExisting(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        ifNonExisting(MarkLogicSourceConfig.DSL_QUERY, dsl);
        ifNonExisting(MarkLogicSourceConfig.TOPIC, topic);

        ifNonExisting(MarkLogicSourceConfig.CONNECTION_HOST, "localhost");
        ifNonExisting(MarkLogicSourceConfig.CONNECTION_PORT, "8000");
        ifNonExisting(MarkLogicSourceConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "DIGEST");
        ifNonExisting(MarkLogicSourceConfig.CONNECTION_USERNAME, "admin");
        ifNonExisting(MarkLogicSourceConfig.CONNECTION_PASSWORD, "admin");
        ifNonExisting("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        ifNonExisting("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        ifNonExisting("errors.log.enable", true);

        final Properties copyOfConnectorProps = new Properties();
        copyOfConnectorProps.putAll(connectorProps);

        return copyOfConnectorProps;
    }

    public static MarkLogicSourceConnectorConfigBuilder create() {
        return new MarkLogicSourceConnectorConfigBuilder();
    }
}

