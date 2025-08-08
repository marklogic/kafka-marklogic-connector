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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class MarkLogicSourceConfigTest extends AbstractIntegrationSourceTest {

    @Test
    void testEmptyConfig() {
        ConfigDef configDef = MarkLogicSourceConfig.CONFIG_DEF;
        Map<String, Object> config = new HashMap<>();
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
        config.put(MarkLogicSourceConfig.CONNECTION_HOST, "localhost");
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
        config.put(MarkLogicSourceConfig.CONNECTION_PORT, "8000");
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
        config.put(MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL);
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
        config.put(MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC);
        configDef.parse(config);
    }

    @Test
    void testWaitTimeConfig() {
        ConfigDef configDef = MarkLogicSourceConfig.CONFIG_DEF;
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.CONNECTION_HOST, "localhost");
        config.put(MarkLogicSourceConfig.CONNECTION_PORT, "8000");
        config.put(MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC);
        config.put(MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL);
        configDef.parse(config);
        config.put(MarkLogicSourceConfig.WAIT_TIME, null);
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
        config.put(MarkLogicSourceConfig.WAIT_TIME, "asdf");
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
        config.put(MarkLogicSourceConfig.WAIT_TIME, -1);
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
        config.put(MarkLogicSourceConfig.WAIT_TIME, 0);
        configDef.parse(config);
    }

    @Test
    void testQueryTypeXor() {
        Assertions.assertThrows(ConfigException.class, () -> startSourceTask(
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC));
        Assertions.assertThrows(ConfigException.class, () -> startSourceTask(
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
                MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
                MarkLogicSourceConfig.SERIALIZED_QUERY, AUTHORS_OPTIC_SERIALIZED));
        Assertions.assertNotNull(startSourceTask(
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
                MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL));
        Assertions.assertNotNull(startSourceTask(
                MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
                MarkLogicSourceConfig.SERIALIZED_QUERY, AUTHORS_OPTIC_SERIALIZED));
    }

    @Test
    void testConstraintPermissions() {
        ConfigDef configDef = MarkLogicSourceConfig.CONFIG_DEF;
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.CONNECTION_HOST, "localhost");
        config.put(MarkLogicSourceConfig.CONNECTION_PORT, "8000");
        config.put(MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC);
        configDef.parse(config);
        config.put(MarkLogicSourceConfig.CONSTRAINT_STORAGE_PERMISSIONS, null);
        configDef.parse(config);
        config.put(MarkLogicSourceConfig.CONSTRAINT_STORAGE_PERMISSIONS, "testRole,read");
        configDef.parse(config);
        config.put(MarkLogicSourceConfig.CONSTRAINT_STORAGE_PERMISSIONS, "testRole,read,anotherRole,update");
        configDef.parse(config);

        config.put(MarkLogicSourceConfig.CONSTRAINT_STORAGE_PERMISSIONS, "asdf");
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
        config.put(MarkLogicSourceConfig.CONSTRAINT_STORAGE_PERMISSIONS, "testRole,read,asdf");
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
        config.put(MarkLogicSourceConfig.CONSTRAINT_STORAGE_PERMISSIONS, "testRole,read,testRole,red");
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
    }

}
