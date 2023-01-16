package com.marklogic.kafka.connect.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String expectedValue = loadTestResourceFileIntoString("serializedAccessorOnlyQuery-expectedResult.json").trim();
        Assertions.assertEquals(expectedValue, serializedQueryHandler.appendConstraintAndOrderByToQuery(originalQuery, constraintValue));
    }

    @Test
    void testSerializedLimitQuerySingleLine() {
        String originalQuery = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}]}}";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};
        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String expectedValue = "{\"$optic\":{\"ns\":\"op\",\"fn\":\"operators\",\"args\":[{\"ns\":\"op\",\"fn\":\"from-view\",\"args\":[\"Medical\",\"Authors\"]},{\"ns\":\"op\",\"fn\":\"where\",\"args\":[{\"ns\":\"op\",\"fn\":\"gt\",\"args\":[{\"ns\":\"op\",\"fn\":\"col\",\"args\":[\"lucky_number\"]},\"52\"]}]},{\"ns\":\"op\",\"fn\":\"order-by\",\"args\":[{\"ns\":\"op\",\"fn\":\"asc\",\"args\":[\"lucky_number\"]}]},{\"ns\":\"op\",\"fn\":\"limit\",\"args\":[1000]}]}}";
        String updatedSerializedQuery = serializedQueryHandler.appendConstraintAndOrderByToQuery(originalQuery, constraintValue);
        updatedSerializedQuery = serializedQueryHandler.appendLimitToQuery(updatedSerializedQuery);
        Assertions.assertEquals(expectedValue, updatedSerializedQuery);
    }

    @Test
    void testSerializedLimitQueryPrettyPrinted() throws IOException {
        String originalQuery = loadTestResourceFileIntoString("serializedQueryWithLimit.json");
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};
        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String expectedValue = loadTestResourceFileIntoString("serializedQueryWithLimit-expectedResult.json").trim();
        String updatedSerializedQuery = serializedQueryHandler.appendConstraintAndOrderByToQuery(originalQuery, constraintValue);
        updatedSerializedQuery = serializedQueryHandler.appendLimitToQuery(updatedSerializedQuery);
        Assertions.assertEquals(expectedValue, updatedSerializedQuery);
    }
}
