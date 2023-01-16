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
        String expectedValue = "op.fromView(\"Medical\", \"Authors\").where(op.gt(op.col('" + constraintColumn + "'), '" + constraintValue + "')).orderBy(op.asc('" + constraintColumn + "'))";
        Assertions.assertEquals(expectedValue, dslQueryHandler.appendConstraintAndOrderByToQuery(constraintValue));
    }

    @Test
    void testQueryWithLimit() {
        String originalDsl = "op.fromView(\"Medical\", \"Authors\")";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.DSL_QUERY, originalDsl);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};
        DslQueryHandler dslQueryHandler = new DslQueryHandler(null, parsedConfig);
        String expectedValue = "op.fromView(\"Medical\", \"Authors\").where(op.gt(op.col('" + constraintColumn + "'), '" + constraintValue + "')).orderBy(op.asc('" + constraintColumn + "')).limit(1000)";
        String actualValue = dslQueryHandler.appendConstraintAndOrderByToQuery(constraintValue);
        actualValue = dslQueryHandler.appendLimitToQuery(actualValue);
        Assertions.assertEquals(expectedValue, actualValue);
    }
}
