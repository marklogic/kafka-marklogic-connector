package com.marklogic.kafka.connect.source;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DslConstraintInjectionTest extends AbstractIntegrationSourceTest {
    private static final String CONSTRAINT_COLUMN = "ID";
    private static final String CONSTRAINT_VALUE = "2";
    private static final Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
        put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, CONSTRAINT_COLUMN);
        put(MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.JSON.toString());
        put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
    }};

    @Test
    void testAccessorOnlyQuery() {
        String userDsl = "op.fromView('Medical','Authors')";
        String expectedValue = userDsl + ".where(op.gt(op.col('ID'), '2')).orderBy(op.asc(op.col('ID')))";
        assertEquals(expectedValue, appendConstraintOntoQuery(userDsl, parsedConfig, "2"));
    }

    @Test
    void wordQueryWithPhraseAndFollowOnFunction() {
        String userDsl = "op.fromDocUris(cts.wordQuery('my phrase').joinDoc('abc'))";
        String expectedResult = userDsl + ".where(op.gt(op.col('ID'), '2')).orderBy(op.asc(op.col('ID')))";
        assertEquals(expectedResult, appendConstraintOntoQuery(userDsl, parsedConfig, "2"),
            "The where clause should have been injected just after the closing paren of the fromDocUris function");
    }

    @Test
    void wordQueryWithOrderBy() {
        String userDsl = "op.fromDocUris(cts.wordQuery('my phrase'))";
        String expectedResult = userDsl + ".where(op.gt(op.col('uri'), '2')).orderBy(op.asc(op.col('uri')))";
        Map<String, Object> localParsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "uri");
            put(MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.JSON.toString());
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
        }};
        assertEquals(expectedResult, appendConstraintOntoQuery(userDsl, localParsedConfig, "2"),
            "The where clause should have been injected just after the closing paren of the fromDocUris function");
    }

    @Test
    void wordQueryWithOrderByNoConstraintValue() throws Exception {
        loadFifteenAuthorsIntoMarkLogic();

        String userDsl = "op.fromDocUris(cts.wordQuery('Moria')).joinDoc(op.col('doc'), op.col('uri'))";
        String expectedResult = userDsl + ".orderBy(op.asc(op.col('uri')))";
        Map<String, Object> localParsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "uri");
            put(MarkLogicSourceConfig.OUTPUT_FORMAT, MarkLogicSourceConfig.OUTPUT_TYPE.JSON.toString());
            put(MarkLogicSourceConfig.ROW_LIMIT, 1000);
            put(MarkLogicSourceConfig.DSL_QUERY, userDsl);
        }};

        startSourceTask(
            MarkLogicSourceConfig.TOPIC, "blah",
            MarkLogicSourceConfig.DSL_QUERY, userDsl
        ).poll();
//        assertEquals(expectedResult, appendConstraintOntoQuery(userDsl, localParsedConfig, null),
//            "The where clause should have been injected just after the closing paren of the fromDocUris function");
//
//        assertUserQueryIsModifiedCorrectly(userDsl, "blah");
    }

    @Test
    void pullDataUsingFromLexicons() throws IOException {
        loadThreeAuthorDocuments();

        String userDsl = "op.fromLexicons({ID:cts.elementReference(xs.QName('ID')),LastName:cts.elementReference(xs.QName('LastName')),})";
        String expectedResult = userDsl + ".where(op.gt(op.col('ID'), '2')).orderBy(op.asc(op.col('ID')))";
        assertUserQueryIsModifiedCorrectly(userDsl, expectedResult);
    }

    @Test
    void pullDataUsingFromLiterals() {
        String userDsl = "op.fromLiterals([{LastName:'Second',ID:2},{LastName:'Third',ID:3},{LastName:'First',ID:1}])";
        String expectedResult = userDsl + ".where(op.gt(op.col('ID'), '2')).orderBy(op.asc(op.col('ID')))";
        assertUserQueryIsModifiedCorrectly(userDsl, expectedResult);
    }

    @Test
    void pullDataUsingFromSQL() throws IOException {
        loadThreeAuthorDocuments();

        String userDsl = "op.fromSQL('SELECT Authors.ID, Authors.LastName FROM Authors')";
        String expectedResult = userDsl + ".where(op.gt(op.col('ID'), '2')).orderBy(op.asc(op.col('ID')))";
        assertUserQueryIsModifiedCorrectly(userDsl, expectedResult);
    }

    @Test
    void pullDataUsingFromView() throws IOException {
        loadThreeAuthorDocuments();

        String userDsl = "op.fromView('Medical','Authors')";
        String expectedResult = userDsl + ".where(op.gt(op.col('ID'), '2')).orderBy(op.asc(op.col('ID')))";

        String result = appendConstraintOntoQuery(userDsl, parsedConfig, "2");
        assertEquals(expectedResult, result,
            "The where clause should have been injected just after the closing paren of the fromView function");

        verifyQueryReturnsExpectedRows(null, 3, "First", parsedConfig);
        verifyQueryReturnsExpectedRows(CONSTRAINT_VALUE, 1, "Third", parsedConfig);
    }

    @Test
    void valueWithSingleQuotes() {
        String expectedQuery = "op.fromView('Medical', 'Authors')" +
            ".where(op.gt(op.col('ID'), 'my odd value'))" +
            ".orderBy(op.asc(op.col('ID')))";
        assertEquals(expectedQuery, injectValue("my 'odd' value"),
            "To prevent the modified query from breaking, single quotes are removed from the previous max value");
    }

    @Test
    void valueWithDoubleQuotes() {
        String expectedQuery = "op.fromView('Medical', 'Authors')" +
            ".where(op.gt(op.col('ID'), 'my odd value'))" +
            ".orderBy(op.asc(op.col('ID')))";
        assertEquals(expectedQuery, injectValue("my \"odd\" value"),
            "To prevent the modified query from breaking, double quotes are removed from the previous max value");
    }

    @Test
    void valueWithParens() {
        String expectedQuery = "op.fromView('Medical', 'Authors')" +
            ".where(op.gt(op.col('ID'), 'my odd value'))" +
            ".orderBy(op.asc(op.col('ID')))";
        assertEquals(expectedQuery, injectValue("my (odd) value"),
            "To prevent the modified query from breaking, parentheses are removed from the previous max value");
    }

    private String injectValue(String value) {
        String originalDsl = "op.fromView('Medical', 'Authors')";
        Map<String, Object> parsedConfig = new HashMap<String, Object>() {{
            put(MarkLogicSourceConfig.DSL_QUERY, originalDsl);
            put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, CONSTRAINT_COLUMN);
        }};
        return appendConstraintOntoQuery(originalDsl, parsedConfig, value);
    }

    private void assertUserQueryIsModifiedCorrectly(String userDsl, String expectedResult) {
        String result = appendConstraintOntoQuery(userDsl, parsedConfig, "2");
        assertEquals(expectedResult, result, "The where clause should have been injected just after the closing paren of the accesor function");

        verifyQueryReturnsExpectedRows(null, 3, "First", parsedConfig);
        verifyQueryReturnsExpectedRows(CONSTRAINT_VALUE, 1, "Third", parsedConfig);
    }
}
