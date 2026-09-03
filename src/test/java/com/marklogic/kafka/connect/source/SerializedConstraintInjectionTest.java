/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
        }};
        JsonNode jsonQuery = objectMapper.readTree(originalQuery);

        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String expectedValue = loadTestResourceFileIntoString("serializedAccessorOnlyQuery-expectedResult.json").trim();
        serializedQueryHandler.appendConstraintAndOrderByToQuery(jsonQuery, constraintValue);
        assertEquals(expectedValue, jsonQuery.toString());
    }

    @Test
    void testSerializedLimitQuerySingleLine() throws IOException {
        String originalQuery = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}]}}";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};
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
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};
        JsonNode jsonQuery = objectMapper.readTree(originalQuery);

        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String expectedValue = loadTestResourceFileIntoString("serializedQueryWithLimit-expectedResult.json").trim();
        serializedQueryHandler.appendConstraintAndOrderByToQuery(jsonQuery, constraintValue);
        serializedQueryHandler.appendLimitToQuery(jsonQuery);
        assertEquals(expectedValue, jsonQuery.toString());
    }

    @Test
    void testNewPlanCalledTwiceProducesIdenticalStructure() {
        String originalQuery = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}]}}";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};

        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String firstQuery = serializedQueryHandler.buildQueryForPlan(null).toString();
        String secondQuery = serializedQueryHandler.buildQueryForPlan(null).toString();

        assertEquals(firstQuery, secondQuery,
            "Calling newPlan()'s query-building logic twice should produce identical structure, " +
                "not accumulate duplicate where/orderBy/limit nodes");
    }

    @Test
    void testNewPlanCalledTwiceWithDifferentConstraintValuesProducesExpectedStructure() {
        String originalQuery = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}]}}";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};

        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        serializedQueryHandler.buildQueryForPlan("10");
        JsonNode secondCycleQuery = serializedQueryHandler.buildQueryForPlan("20");

        String expectedValue = "{\"$optic\":{\"ns\":\"op\",\"fn\":\"operators\",\"args\":[{\"ns\":\"op\",\"fn\":\"from-view\",\"args\":[\"Medical\",\"Authors\"]},{\"ns\":\"op\",\"fn\":\"where\",\"args\":[{\"ns\":\"op\",\"fn\":\"gt\",\"args\":[{\"ns\":\"op\",\"fn\":\"col\",\"args\":[\"lucky_number\"]},\"20\"]}]},{\"ns\":\"op\",\"fn\":\"order-by\",\"args\":[{\"ns\":\"op\",\"fn\":\"asc\",\"args\":[\"lucky_number\"]}]},{\"ns\":\"op\",\"fn\":\"limit\",\"args\":[1000]}]}}";
        assertEquals(expectedValue, secondCycleQuery.toString(),
            "The second call's query should only contain the second call's constraint value, not both stacked");
    }

    @Test
    void testGetMaxConstraintColumnValueDoesNotCorruptSubsequentNewPlan() {
        String originalQuery = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}]}}";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};

        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String firstQuery = serializedQueryHandler.buildQueryForPlan(constraintValue).toString();
        serializedQueryHandler.buildMaxValueSerializedQuery();
        String thirdQuery = serializedQueryHandler.buildQueryForPlan(constraintValue).toString();

        assertEquals(firstQuery, thirdQuery,
            "A newPlan() call after getMaxConstraintColumnValue() should match the first newPlan() call, " +
                "with no leftover orderBy(desc)/limit(1)/select nodes from the max-value computation");
    }
}
