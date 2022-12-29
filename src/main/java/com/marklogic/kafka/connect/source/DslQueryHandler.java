package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.RowBatcher;
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RawQueryDSLPlan;
import com.marklogic.client.row.RowManager;
import org.springframework.util.StringUtils;

import java.util.Map;

public class DslQueryHandler extends LoggingObject implements QueryHandler {

    private final DatabaseClient databaseClient;
    private final String userDslQuery;
    private final String constraintColumnName;

    private String currentDslQuery = null;

    public DslQueryHandler(DatabaseClient databaseClient, Map<String, Object> parsedConfig) {
        this.databaseClient = databaseClient;
        this.userDslQuery = (String) parsedConfig.get(MarkLogicSourceConfig.DSL_QUERY);
        String rawConstraintColumnName = (String) parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME);
        if (!StringUtils.hasText(rawConstraintColumnName)) {
            constraintColumnName = null;
        } else {
            constraintColumnName = QueryHandlerUtil.sanitize(rawConstraintColumnName);
        }
    }

    public void addQueryToRowBatcher(RowBatcher<?> rowBatcher, String previousMaxConstraintColumnValue) {
        currentDslQuery = injectConstraintIntoQuery(previousMaxConstraintColumnValue);
        RowManager rowMgr = rowBatcher.getRowManager();
        RawQueryDSLPlan query = rowMgr.newRawQueryDSLPlan(new StringHandle(currentDslQuery));
        rowBatcher.withBatchView(query);
    }

    protected String injectConstraintIntoQuery(String previousMaxConstraintColumnValue) {
        String constrainedDsl = userDslQuery;
        if (previousMaxConstraintColumnValue != null) {
            String constraintPhrase = ".where(op.gt(op.col(\"" + constraintColumnName + "\"), \"" + previousMaxConstraintColumnValue + "\"))";
            String originalDslNoWhitespace = QueryHandlerUtil.removeWhitespace(userDslQuery);
            if (originalDslNoWhitespace.contains(").")) {
                int firstClosingParen = originalDslNoWhitespace.indexOf(").");
                constrainedDsl = originalDslNoWhitespace.substring(0, firstClosingParen + 1)
                    + constraintPhrase + originalDslNoWhitespace.substring(firstClosingParen + 1);
            } else {
                constrainedDsl = userDslQuery + constraintPhrase;
            }
        }
        logger.debug("DSL constrainedQuery (unless initial run): " + constrainedDsl);
        return constrainedDsl;
    }

    public String updatePreviousMaxConstraintColumnValue(long queryStartTimeInMillis) {
        String currentMaxValueQuery = buildMaxValueDslQuery();
        logger.debug("currentMaxValueQuery: " + currentMaxValueQuery);
        RowManager rowMgr = databaseClient.newRowManager();
        RawQueryDSLPlan maxConstraintValueQuery = rowMgr.newRawQueryDSLPlan(new StringHandle(currentMaxValueQuery));
        JacksonHandle handle = new JacksonHandle().withFormat(Format.JSON).withMimetype("application/json");
        handle.setPointInTimeQueryTimestamp(queryStartTimeInMillis);
        JacksonHandle result = rowMgr.resultDoc(maxConstraintValueQuery, handle);
        String previousMaxConstraintColumnValue = null;
        try {
            String rawMaxConstraintColumnValue = result.get().get("rows").get(0).get("constraint").get("value").asText();
            previousMaxConstraintColumnValue = QueryHandlerUtil.sanitize(rawMaxConstraintColumnValue);
        } catch (Exception ex) {
            logger.warn("Failed to get a valid Maximum Constraint value: " + ex.getMessage());
        }
        return previousMaxConstraintColumnValue;
    }

    private String buildMaxValueDslQuery() {
        return String.format("%s.orderBy(op.desc(\"%s\")).limit(1).select([op.as(\"constraint\", op.col(\"%s\"))])", currentDslQuery, constraintColumnName, constraintColumnName);
    }
}
