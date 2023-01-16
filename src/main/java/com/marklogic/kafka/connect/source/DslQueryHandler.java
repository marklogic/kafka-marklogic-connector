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

    private final static String VALUE_SANITIZATION_PATTERN = "[\"'\\(\\)]";

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
        this.currentDslQuery = constrainUserQuery(previousMaxConstraintColumnValue);
        if (logger.isDebugEnabled()) {
            logger.debug("DSL query: " + currentDslQuery);
        }
        return databaseClient.newRowManager().newRawQueryDSLPlan(new StringHandle(currentDslQuery));
    }

    /**
     * Constrains the original user query based on the existence of constraint column and row limit parameters.
     *
     * Unfortunately, string concatenation is currently required here. The query cannot be modified programmatically
     * since the RawPlan hierarchy is separate from the ModifyPlan hierarchy. It would be feasible to use bindParam if
     * we ask the user to define the type of the column. Based on what bindParam supports, that type would be one of:
     * byte, int, long, short, float, double, boolean, String. String would be a sensible default since it's expected
     * that date/dateTime columns will be most frequently used. But the UX of this is a bit awkward, particularly
     * since a user only needs to specify this when using a DSL and not a serialized plan (as the value can be safely
     * stored within the JSON document, and so there's no need for bindParam). Instead, the user's value is being
     * sanitized to avoid breaking the modifying the original user query.
     *
     * @param previousMaxConstraintColumnValue
     * @return
     */
    protected String constrainUserQuery(String previousMaxConstraintColumnValue) {
        String constrainedDsl = userDslQuery;
        if (previousMaxConstraintColumnValue != null) {
            String sanitizedValue = previousMaxConstraintColumnValue.replaceAll(VALUE_SANITIZATION_PATTERN, "");
            constrainedDsl = String.format("%s" +
                    ".where(op.gt(op.col('%s'), '%s'))" +
                    ".orderBy(op.asc(op.col('%s')))",
                userDslQuery, constraintColumnName, sanitizedValue, constraintColumnName
            );
        }
        if (rowLimit != null && rowLimit > 0) {
            constrainedDsl += String.format(".limit(%d)", rowLimit);
        }
        return constrainedDsl;
    }

    public String getMaxConstraintColumnValue(long serverTimestamp) {
        String maxValueQuery = buildMaxValueDslQuery();
        if (logger.isDebugEnabled()) {
            logger.debug("Query for max constraint value: " + maxValueQuery);
        }
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
        return String.format("%s" +
                ".orderBy(op.desc(op.col('%s')))" +
                ".limit(1)" +
                ".select([op.as('constraint', op.col('%s'))])",
            currentDslQuery, constraintColumnName, constraintColumnName);
    }

    public String getCurrentQuery() {
        return currentDslQuery;
    }
}
