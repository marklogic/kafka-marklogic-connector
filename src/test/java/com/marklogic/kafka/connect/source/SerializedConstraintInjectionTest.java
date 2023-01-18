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
}
