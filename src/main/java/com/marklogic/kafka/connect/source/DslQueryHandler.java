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
        RowManager rowManager = databaseClient.newRowManager();
        PlanBuilder.Plan plan = rowManager.newRawQueryDSLPlan(new StringHandle(currentDslQuery));
        PlanBuilder op = rowManager.newPlanBuilder();
        if (rowLimit > 0) {
            plan = plan.bindParam(op.param("ROW_LIMIT"), rowLimit);
        }
        if (previousMaxConstraintColumnValue != null) {
            plan = plan.bindParam(op.param("PREVIOUS_MAX_VALUE"), previousMaxConstraintColumnValue);
        }
        return plan;
    }

    protected String appendLimitToQuery(String currentDslQuery) {
        return currentDslQuery + ".limit(op.param('ROW_LIMIT'))";
    }

    protected String appendConstraintAndOrderByToQuery(String previousMaxConstraintColumnValue) {
        String constrainedDsl = userDslQuery;
        if (constraintColumnName != null) {
            if (previousMaxConstraintColumnValue != null) {
                constrainedDsl += String.format(".where(op.gt(op.col('%s'), op.param('PREVIOUS_MAX_VALUE')))", constraintColumnName);
            }
            constrainedDsl += String.format(".orderBy(op.asc(op.col('%s')))", constraintColumnName);
        }
        return constrainedDsl;
//        String constrainedDsl = userDslQuery;
//        if (previousMaxConstraintColumnValue != null) {
//            String constraintPhrase = ".where(op.gt(op.col('" + constraintColumnName + "'), op.col('previousMaxConstraintColumnValue'))"
//                + ".orderBy(op.asc('" + constraintColumnName + "'))";
//            constrainedDsl = userDslQuery + constraintPhrase;
//        }
//        return constrainedDsl;
    }

    @Override
    public String getMaxConstraintColumnValue(long serverTimestamp, String previousMaxConstraintColumnValue) {
        String maxValueQuery = buildMaxValueDslQuery();
        logger.info("Query for max constraint value: " + maxValueQuery);
        RowManager rowManager = databaseClient.newRowManager();
        PlanBuilder.Plan plan = rowManager.newRawQueryDSLPlan(new StringHandle(maxValueQuery));
        PlanBuilder op = rowManager.newPlanBuilder();
        if (previousMaxConstraintColumnValue != null) {
            plan = plan.bindParam(op.param("PREVIOUS_MAX_VALUE"), previousMaxConstraintColumnValue);
        }
        if (rowLimit != null) {
            plan = plan.bindParam(op.param("ROW_LIMIT"), rowLimit);
        }
        JacksonHandle handle = new JacksonHandle();
        handle.setPointInTimeQueryTimestamp(serverTimestamp);
        JacksonHandle result = rowManager.resultDoc(plan, handle);
        /**
         * We get back the type info here with JSON, which in theory would allow us to call the proper method when
         * we do a bind later.
         */
        System.out.println("MAX VALUE RESULT: " + result.get().toPrettyString());
        try {
            String rawMaxConstraintColumnValue = result.get().get("rows").get(0).get("constraint").get("value").asText();
            return QueryHandlerUtil.sanitize(rawMaxConstraintColumnValue);
        } catch (Exception ex) {
            throw new RuntimeException("Unable to get max constraint value; query: " + plan +
                    "; response: " + result + "; cause: " + ex.getMessage());
        }
    }

    private String buildMaxValueDslQuery() {
        // Odd - when using select, bindParam doesn't work when using a string when a number is required
        // But when we just bind a new column, everything is fine
        return String.format("%s" +
                ".orderBy(op.desc('%s'))" +
                ".limit(1)" +
                ".bind(op.as('constraint', op.col('%s')))",
            currentDslQuery, constraintColumnName, constraintColumnName);
    }

    public String getCurrentQuery() {
        return currentDslQuery;
    }
}
