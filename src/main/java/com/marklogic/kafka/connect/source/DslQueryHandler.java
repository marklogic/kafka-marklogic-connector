package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RawQueryDSLPlan;
import com.marklogic.client.row.RowManager;
import org.springframework.util.StringUtils;

import java.util.Map;


public class DslQueryHandler extends LoggingObject implements QueryHandler {

    private final static String VALUE_SANITIZATION_PATTERN = "[\"'\\(\\)]";

    private final DatabaseClient databaseClient;
    private final String userDslQuery;
    private final String constraintColumnName;
    private final Integer rowLimit;

    private String currentDslQuery;

    public DslQueryHandler(DatabaseClient databaseClient, Map<String, Object> parsedConfig) {
        this.databaseClient = databaseClient;
        this.userDslQuery = (String) parsedConfig.get(MarkLogicSourceConfig.DSL_QUERY);
        this.constraintColumnName = (String) parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME);
        rowLimit = (Integer) parsedConfig.get(MarkLogicSourceConfig.ROW_LIMIT);
    }

    @Override
    public PlanBuilder.Plan newPlan(String previousMaxConstraintColumnValue) {
        currentDslQuery = appendConstraintAndOrderByToQuery(previousMaxConstraintColumnValue);
        if (rowLimit > 0) {
            currentDslQuery += ".limit(" + rowLimit + ")";
        }
        logger.debug("DSL query: {}", currentDslQuery);
        return databaseClient.newRowManager().newRawQueryDSLPlan(new StringHandle(currentDslQuery));
    }

    protected String appendConstraintAndOrderByToQuery(String previousMaxConstraintColumnValue) {
        String constrainedDsl = userDslQuery;
        if (constraintColumnName != null) {
            String constraintPhrase = "";
            if (previousMaxConstraintColumnValue != null) {
                String sanitizedValue = previousMaxConstraintColumnValue.replaceAll(VALUE_SANITIZATION_PATTERN, "");
                constraintPhrase = String.format(".where(op.gt(op.col('%s'), '%s'))", constraintColumnName, sanitizedValue);
            }
            constraintPhrase += ".orderBy(op.asc('" + constraintColumnName + "'))";
            constrainedDsl = userDslQuery + constraintPhrase;
        }
        return constrainedDsl;
    }

    @Override
    public String getMaxConstraintColumnValue(long serverTimestamp) {
        String maxValueQuery = buildMaxValueDslQuery();
        logger.debug("Query for max constraint value: {}", maxValueQuery);
        RowManager rowMgr = databaseClient.newRowManager();
        RawQueryDSLPlan maxConstraintValueQuery = rowMgr.newRawQueryDSLPlan(new StringHandle(maxValueQuery));
        JacksonHandle handle = new JacksonHandle().withFormat(Format.JSON).withMimetype("application/json");
        handle.setPointInTimeQueryTimestamp(serverTimestamp);
        JacksonHandle result = rowMgr.resultDoc(maxConstraintValueQuery, handle);
        try {
            return result.get().get("rows").get(0).get("constraint").get("value").asText();
        } catch (Exception ex) {
            throw new RuntimeException("Unable to get max constraint value; query: " + maxConstraintValueQuery +
                "; response: " + result + "; cause: " + ex.getMessage());
        }
    }

    private String buildMaxValueDslQuery() {
        return String.format("%s.orderBy(op.desc(\"%s\")).limit(1).select([op.as(\"constraint\", op.col(\"%s\"))])", currentDslQuery, constraintColumnName, constraintColumnName);
    }

    public String getCurrentQuery() {
        return currentDslQuery;
    }
}
