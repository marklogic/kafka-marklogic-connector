/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        ));
        Assertions.assertThrows(ConfigException.class, () -> startSourceTask(
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.SERIALIZED_QUERY, AUTHORS_OPTIC_SERIALIZED
        ));
        Assertions.assertNotNull(startSourceTask(
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL
        ));
        Assertions.assertNotNull(startSourceTask(
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.SERIALIZED_QUERY, AUTHORS_OPTIC_SERIALIZED
        ));
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

    @Test
    void testConstraintColumnNameValidation() {
        ConfigDef configDef = MarkLogicSourceConfig.CONFIG_DEF;
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.CONNECTION_HOST, "localhost");
        config.put(MarkLogicSourceConfig.CONNECTION_PORT, "8000");
        config.put(MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL);
        config.put(MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC);

        // Valid constraint column names
        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, null);
        configDef.parse(config); // null is allowed (optional field)
        
        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "ID");
        configDef.parse(config); // alphanumeric

        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "id_123");
        configDef.parse(config); // with underscore

        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "col.name");
        configDef.parse(config); // with dot

        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "col-name");
        configDef.parse(config); // with hyphen

        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "a");
        configDef.parse(config); // single character

        // Invalid constraint column names (DSL injection attempts)
        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "')) .joinInner(op.fromView('sensitive', 'secrets')).select([op.col('secret_value");
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config),
            "Should reject column names with quotes and special characters");

        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "id'; DROP TABLE users;");
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config),
            "Should reject column names with semicolons and SQL injection attempts");

        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "col(name)");
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config),
            "Should reject column names with parentheses");

        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "col@name");
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config),
            "Should reject column names with special characters");

        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "col name");
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config),
            "Should reject column names with spaces");

        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "");
        configDef.parse(config); // empty string is treated as unset

        // Test max length boundary
        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "a".repeat(128));
        configDef.parse(config); // exactly 128 characters is allowed

        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "a".repeat(129));
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config),
            "Should reject column names exceeding 128 characters");
    }

}
