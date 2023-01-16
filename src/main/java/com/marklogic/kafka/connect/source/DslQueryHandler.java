package com.marklogic.kafka.connect.source;

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

    private final DatabaseClient databaseClient;
    private final String userDslQuery;
    private final String constraintColumnName;
    private final Integer rowLimit;

    private String currentDslQuery;

    public DslQueryHandler(DatabaseClient databaseClient, Map<String, Object> parsedConfig) {
        this.databaseClient = databaseClient;
        this.userDslQuery = (String) parsedConfig.get(MarkLogicSourceConfig.DSL_QUERY);
        String rawConstraintColumnName = (String) parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME);
        if (!StringUtils.hasText(rawConstraintColumnName)) {
            constraintColumnName = null;
        } else {
            constraintColumnName = QueryHandlerUtil.sanitize(rawConstraintColumnName);
        }
        rowLimit = (Integer) parsedConfig.get(MarkLogicSourceConfig.ROW_LIMIT);
    }

    @Override
    public PlanBuilder.Plan newPlan(String previousMaxConstraintColumnValue) {
        currentDslQuery = appendConstraintAndOrderByToQuery(previousMaxConstraintColumnValue);
        if (rowLimit > 0) {
            currentDslQuery = appendLimitToQuery(currentDslQuery);
        }
        logger.info("DSL query: " + currentDslQuery);
        return databaseClient.newRowManager().newRawQueryDSLPlan(new StringHandle(currentDslQuery));
    }

    protected String appendLimitToQuery(String currentDslQuery) {
        return currentDslQuery + ".limit(" + rowLimit + ")";
    }

    protected String appendConstraintAndOrderByToQuery(String previousMaxConstraintColumnValue) {
        String constrainedDsl = userDslQuery;
        if (previousMaxConstraintColumnValue != null) {
            String constraintPhrase = ".where(op.gt(op.col('" + constraintColumnName + "'), '" + previousMaxConstraintColumnValue + "'))"
                + ".orderBy(op.asc('" + constraintColumnName + "'))";
            constrainedDsl = userDslQuery + constraintPhrase;
        }
        return constrainedDsl;
    }

    public String getMaxConstraintColumnValue(long serverTimestamp) {
        String maxValueQuery = buildMaxValueDslQuery();
        logger.info("Query for max constraint value: " + maxValueQuery);
        RowManager rowMgr = databaseClient.newRowManager();
        RawQueryDSLPlan maxConstraintValueQuery = rowMgr.newRawQueryDSLPlan(new StringHandle(maxValueQuery));
        JacksonHandle handle = new JacksonHandle().withFormat(Format.JSON).withMimetype("application/json");
        handle.setPointInTimeQueryTimestamp(serverTimestamp);
        JacksonHandle result = rowMgr.resultDoc(maxConstraintValueQuery, handle);
        try {
            String rawMaxConstraintColumnValue = result.get().get("rows").get(0).get("constraint").get("value").asText();
            return QueryHandlerUtil.sanitize(rawMaxConstraintColumnValue);
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
