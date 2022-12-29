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
        String expectedValue = loadTestResourceFileIntoString("serializedAccessorOnlyQuery-expectedResult.json");
        expectedValue = QueryHandlerUtil.removeWhitespace(expectedValue);
        Assertions.assertEquals(expectedValue, serializedQueryHandler.injectConstraintIntoQuery(constraintValue));
    }

    @Test
    void testSerializedLimitQuerySingleLine() {
        String originalQuery = "{\"$optic\":{\"ns\":\"op\", \"fn\":\"operators\", \"args\":[{\"ns\":\"op\", \"fn\":\"from-view\", \"args\":[\"Medical\", \"Authors\"]}, {\"ns\":\"op\", \"fn\":\"limit\", \"args\":[1000]}]}}";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
        }};
        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String expectedValue = "{\"$optic\":{\"ns\":\"op\",\"fn\":\"operators\",\"args\":[{\"ns\":\"op\",\"fn\":\"from-view\",\"args\":[\"Medical\",\"Authors\"]},{\"ns\":\"op\",\"fn\":\"where\",\"args\":[{\"ns\":\"op\",\"fn\":\"gt\",\"args\":[{\"ns\":\"op\",\"fn\":\"col\",\"args\":[\"lucky_number\"]},\"52\"]}]},{\"ns\":\"op\",\"fn\":\"limit\",\"args\":[1000]}]}}";
        Assertions.assertEquals(expectedValue, serializedQueryHandler.injectConstraintIntoQuery(constraintValue));
    }

    @Test
    void testSerializedLimitQueryPrettyPrinted() throws IOException {
        String originalQuery = loadTestResourceFileIntoString("serializedQueryWithLimit.json");
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
        }};
        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String expectedValue = loadTestResourceFileIntoString("serializedQueryWithLimit-expectedResult.json");
        expectedValue = QueryHandlerUtil.removeWhitespace(expectedValue);
        Assertions.assertEquals(expectedValue, serializedQueryHandler.injectConstraintIntoQuery(constraintValue));
    }

    @Test
    void testQueryWithSpacesInQuotes() throws IOException {
        String originalQuery = loadTestResourceFileIntoString("serializedQueryWithSpacesInQuotes.json");
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.SERIALIZED_QUERY, originalQuery);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
        }};
        SerializedQueryHandler serializedQueryHandler = new SerializedQueryHandler(null, parsedConfig);
        String expectedValue = loadTestResourceFileIntoString("serializedQueryWithSpacesInQuotes-expectedResult.json");
        expectedValue = QueryHandlerUtil.removeWhitespace(expectedValue);
        Assertions.assertEquals(expectedValue, serializedQueryHandler.injectConstraintIntoQuery(constraintValue));
    }
}
