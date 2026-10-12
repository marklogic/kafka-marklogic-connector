/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarkLogicConfigValidatorTest {

    @Test
    void recommenderReturnsAllowedValuesAndIsVisible() {
        MarkLogicConfig.CustomRecommenderAndValidator validator =
            new MarkLogicConfig.CustomRecommenderAndValidator("DIGEST", "BASIC", "NONE");

        List<Object> values = validator.validValues("securityContextType", new HashMap<>());
        assertEquals(List.of("DIGEST", "BASIC", "NONE"), values);
        assertTrue(validator.visible("securityContextType", new HashMap<>()));
    }

    @Test
    void validatorAcceptsValuesCaseInsensitivelyAndRejectsUnknownValues() {
        MarkLogicConfig.CustomRecommenderAndValidator validator =
            new MarkLogicConfig.CustomRecommenderAndValidator("DIGEST", "BASIC", "NONE");

        validator.ensureValid("securityContextType", "basic");
        ConfigException error = assertThrows(ConfigException.class,
            () -> validator.ensureValid("securityContextType", "OAUTH"));
        assertTrue(error.getMessage().contains("OAUTH"));
    }

    @Test
    void validatorRejectsValuesThatAreNotStrings() {
        MarkLogicConfig.CustomRecommenderAndValidator validator =
            new MarkLogicConfig.CustomRecommenderAndValidator("DIGEST", "BASIC", "NONE");

        assertThrows(ConfigException.class, () -> validator.ensureValid("securityContextType", null));
        assertThrows(ConfigException.class, () -> validator.ensureValid("securityContextType", 42));
    }
}
