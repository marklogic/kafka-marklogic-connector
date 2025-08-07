/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        if (connectorProps.get(propertyName) != null)
            return;
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
