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

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SerializedConstraintInjectionTest extends AbstractIntegrationSourceTest {
    private static final String constraintColumn = "lucky_number";
    private static final String constraintValue = "52";

    @Test
    void testAccessorOnlyQuery() throws IOException {
        String originalQuery = loadTestResourceFileIntoString("serializedAccessorOnlyQuery.json");
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {
            {
                put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
                put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
            }
        };
        JsonNode jsonQuery = objectMapper.readTree(originalQuery);

        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String expectedValue = loadTestResourceFileIntoString("serializedAccessorOnlyQuery-expectedResult.json").trim();
        serializedQueryHandler.appendConstraintAndOrderByToQuery(jsonQuery, constraintValue);
        assertEquals(expectedValue, jsonQuery.toString());
    }

    @Test
    void testSerializedLimitQuerySingleLine() throws IOException {
        String originalQuery = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}]}}";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {
            {
                put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
                put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
                put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
            }
        };
        JsonNode jsonQuery = objectMapper.readTree(originalQuery);

        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String expectedValue = "{\"$optic\":{\"ns\":\"op\",\"fn\":\"operators\",\"args\":[{\"ns\":\"op\",\"fn\":\"from-view\",\"args\":[\"Medical\",\"Authors\"]},{\"ns\":\"op\",\"fn\":\"where\",\"args\":[{\"ns\":\"op\",\"fn\":\"gt\",\"args\":[{\"ns\":\"op\",\"fn\":\"col\",\"args\":[\"lucky_number\"]},\"52\"]}]},{\"ns\":\"op\",\"fn\":\"order-by\",\"args\":[{\"ns\":\"op\",\"fn\":\"asc\",\"args\":[\"lucky_number\"]}]},{\"ns\":\"op\",\"fn\":\"limit\",\"args\":[1000]}]}}";
        serializedQueryHandler.appendConstraintAndOrderByToQuery(jsonQuery, constraintValue);
        serializedQueryHandler.appendLimitToQuery(jsonQuery);
        assertEquals(expectedValue, jsonQuery.toString());
    }

    @Test
    void testSerializedLimitQueryPrettyPrinted() throws IOException {
        String originalQuery = loadTestResourceFileIntoString("serializedQueryWithLimit.json");
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {
            {
                put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
                put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
                put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
            }
        };
        JsonNode jsonQuery = objectMapper.readTree(originalQuery);

        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String expectedValue = loadTestResourceFileIntoString("serializedQueryWithLimit-expectedResult.json").trim();
        serializedQueryHandler.appendConstraintAndOrderByToQuery(jsonQuery, constraintValue);
        serializedQueryHandler.appendLimitToQuery(jsonQuery);
        assertEquals(expectedValue, jsonQuery.toString());
    }
}
