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
        config.put(MarkLogicSourceConfig.DSL_PLAN, AUTHORS_OPTIC_DSL);
        configDef.parse(config);
        config.put(MarkLogicSourceConfig.DSL_PLAN, null);
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
    }

    @Test
    void testDslConfig() {
        ConfigDef configDef = MarkLogicSourceConfig.CONFIG_DEF;
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.CONNECTION_HOST, "localhost");
        config.put(MarkLogicSourceConfig.CONNECTION_PORT, "8000");
        config.put(MarkLogicSourceConfig.DSL_PLAN, null);
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
        config.put(MarkLogicSourceConfig.DSL_PLAN, 1);
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
        config.put(MarkLogicSourceConfig.DSL_PLAN, "");
        Assertions.assertThrows(ConfigException.class, () -> configDef.parse(config));
        config.put(MarkLogicSourceConfig.DSL_PLAN, AUTHORS_OPTIC_DSL);
        configDef.parse(config);
    }
}
