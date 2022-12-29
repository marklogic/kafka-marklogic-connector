package com.marklogic.kafka.connect.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class DslConstraintInjectionTest extends AbstractIntegrationSourceTest {
    private static final String constraintColumn = "lucky_number";
    private static final String constraintValue = "52";

    @Test
    void testAccessorOnlyQuery() {
        String originalDsl = "op.fromView(\"Medical\", \"Authors\")";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.DSL_QUERY, originalDsl);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
        }};
        DslQueryHandler dslQueryHandler = new DslQueryHandler(null, parsedConfig);
        String expectedValue = "op.fromView(\"Medical\", \"Authors\").where(op.gt(op.col(\"" + constraintColumn + "\"), \"" + constraintValue + "\"))";
        Assertions.assertEquals(expectedValue, dslQueryHandler.injectConstraintIntoQuery(constraintValue));
    }

    @Test
    void testQueryWithLimit() {
        String originalDsl = "op.fromView(\"Medical\", \"Authors\").limit(1000)";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.DSL_QUERY, originalDsl);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
        }};
        DslQueryHandler dslQueryHandler = new DslQueryHandler(null, parsedConfig);
        String expectedValue = "op.fromView(\"Medical\",\"Authors\").where(op.gt(op.col(\"" + constraintColumn + "\"), \"" + constraintValue + "\")).limit(1000)";
        Assertions.assertEquals(expectedValue, dslQueryHandler.injectConstraintIntoQuery(constraintValue));
    }

    @Test
    void testQueryWithLineFeed() {
        String originalDsl = "op.fromView(\"Medical\", \"Authors\")  \n    .limit(1000)";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.DSL_QUERY, originalDsl);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
        }};
        DslQueryHandler dslQueryHandler = new DslQueryHandler(null, parsedConfig);
        String expectedValue = "op.fromView(\"Medical\",\"Authors\").where(op.gt(op.col(\"" + constraintColumn + "\"), \"" + constraintValue + "\")).limit(1000)";
        Assertions.assertEquals(expectedValue, dslQueryHandler.injectConstraintIntoQuery(constraintValue));
    }

    @Test
    void testQueryWithSpacesInSingleQuotes() {
        String originalDsl = "op.fromView(\"Medical\", \"Authors\").select([op.as(\"Last Name\", op.col(\"LastName\"), op.as(\'Fore Name\', op.col(\'ForeName\')])";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.DSL_QUERY, originalDsl);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
        }};
        DslQueryHandler dslQueryHandler = new DslQueryHandler(null, parsedConfig);
        String expectedValue = "op.fromView(\"Medical\",\"Authors\").where(op.gt(op.col(\"" + constraintColumn + "\"), \"" +
            constraintValue + "\")).select([op.as(\"Last Name\",op.col(\"LastName\"),op.as(\'Fore Name\',op.col(\'ForeName\')])";
        Assertions.assertEquals(expectedValue, dslQueryHandler.injectConstraintIntoQuery(constraintValue));
    }
}
