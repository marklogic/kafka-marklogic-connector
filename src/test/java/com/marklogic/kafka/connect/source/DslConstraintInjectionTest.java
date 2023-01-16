package com.marklogic.kafka.connect.source;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This includes some tests that ensure that certain characters in the previous max value for a constraint column do
 * not cause the modified user query to break. This also prevents possible "optic injection" attacks, though those
 * seem extremely unusual given that the user already has control over the original query. In theory, a malicious user
 * without access to the Kafka connector config but with the ability to insert data could insert a malicious value
 * that allows for modifying the Optic query. This would require a string index on the constraint column, which itself
 * seems highly unusual. And, in order to modify any data, the initial query would need to be an Optic Update query,
 * and the MarkLogic user running the query would need to have the ability to modify data. So it's extremely remote
 * that there's an attack vector here, but the previous max value is nonetheless sanitized.
 */
class DslConstraintInjectionTest extends AbstractIntegrationSourceTest {
    private static final String constraintColumn = "my_column";
    private static final String constraintValue = "52";

    @Test
    void testAccessorOnlyQuery() {
        String expectedValue = "op.fromView('Medical', 'Authors')" +
            ".where(op.gt(op.col('" + constraintColumn + "'), '" + constraintValue + "'))" +
            ".orderBy(op.asc(op.col('" + constraintColumn + "')))";
        assertEquals(expectedValue, injectValue(constraintValue));
    }

    @Test
    void testQueryWithLimit() {
        String expectedValue = "op.fromView('Medical', 'Authors')" +
            ".where(op.gt(op.col('" + constraintColumn + "'), '" + constraintValue + "'))" +
            ".orderBy(op.asc(op.col('" + constraintColumn + "'))).limit(1000)";
        assertEquals(expectedValue, injectValue(constraintValue, 1000));
    }

    @Test
    void valueWithSingleQuotes() {
        String expectedQuery = "op.fromView('Medical', 'Authors')" +
            ".where(op.gt(op.col('my_column'), 'my odd value'))" +
            ".orderBy(op.asc(op.col('my_column')))";
        assertEquals(expectedQuery, injectValue("my 'odd' value"),
            "To prevent the modified query from breaking, single quotes are removed from the previous max value");
    }

    @Test
    void valueWithDoubleQuotes() {
        String expectedQuery = "op.fromView('Medical', 'Authors')" +
            ".where(op.gt(op.col('my_column'), 'my odd value'))" +
            ".orderBy(op.asc(op.col('my_column')))";
        assertEquals(expectedQuery, injectValue("my \"odd\" value"),
            "To prevent the modified query from breaking, double quotes are removed from the previous max value");
    }

    @Test
    void valueWithParens() {
        String expectedQuery = "op.fromView('Medical', 'Authors')" +
            ".where(op.gt(op.col('my_column'), 'my odd value'))" +
            ".orderBy(op.asc(op.col('my_column')))";
        assertEquals(expectedQuery, injectValue("my (odd) value"),
            "To prevent the modified query from breaking, parentheses are removed from the previous max value");
    }

    private String injectValue(String value) {
        return injectValue(value, null);
    }

    private String injectValue(String value, Integer rowLimit) {
        String originalDsl = "op.fromView('Medical', 'Authors')";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.DSL_QUERY, originalDsl);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumn);
        }};
        if (rowLimit != null) {
            parsedConfig.put(MarkLogicSourceConfig.ROW_LIMIT, rowLimit);
        }
        return new DslQueryHandler(null, parsedConfig).constrainUserQuery(value);
    }
}
