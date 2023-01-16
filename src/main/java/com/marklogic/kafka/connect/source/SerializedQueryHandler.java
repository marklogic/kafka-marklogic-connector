package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RawPlanDefinition;
import com.marklogic.client.row.RowManager;
import org.springframework.util.StringUtils;

import java.util.Map;

public class SerializedQueryHandler extends LoggingObject implements QueryHandler {
    static private final ObjectMapper mapper = new ObjectMapper();

    private final DatabaseClient databaseClient;
    private final String userSerializedQuery;
    private final String constraintColumnName;
    private final Integer rowLimit;

    private String currentSerializedQuery = null;

    public SerializedQueryHandler(DatabaseClient databaseClient, Map<String, Object> parsedConfig) {
        this.databaseClient = databaseClient;
        this.userSerializedQuery = (String) parsedConfig.get(MarkLogicSourceConfig.SERIALIZED_QUERY);
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
        currentSerializedQuery = appendConstraintAndOrderByToQuery(userSerializedQuery, previousMaxConstraintColumnValue);
        if (rowLimit > 0) {
            currentSerializedQuery = appendLimitToQuery(currentSerializedQuery);
        }
        logger.info("Serialized query: " + currentSerializedQuery);
        return databaseClient.newRowManager().newRawPlanDefinition(new StringHandle(currentSerializedQuery));
    }

    protected String appendLimitToQuery(String currentSerializedQuery) {
        ObjectNode limitNode = buildLimitNode(rowLimit);
        try {
            ObjectNode queryRoot = (ObjectNode) mapper.readTree(currentSerializedQuery);
            ArrayNode rootArgsArray = (ArrayNode) queryRoot.get("$optic").get("args");

            rootArgsArray.add(limitNode);
            return mapper.writeValueAsString(queryRoot);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to modify serialized query to include the constraint column, cause: " + e.getMessage(), e);
        }
    }

    protected String appendConstraintAndOrderByToQuery(String currentSerializedQuery, String previousMaxConstraintColumnValue) {
        String constrainedSerialized = currentSerializedQuery;
        if (constraintColumnName != null) {
            try {
                ObjectNode orderByNode = buildOrderByNode(true);
                ObjectNode queryRoot = (ObjectNode) mapper.readTree(currentSerializedQuery);
                ArrayNode rootArgsArray = (ArrayNode) queryRoot.get("$optic").get("args");

                if (previousMaxConstraintColumnValue != null) {
                    ObjectNode constraintNode = buildConstraintNode(previousMaxConstraintColumnValue);
                    rootArgsArray.add(constraintNode);
                }

                rootArgsArray.add(orderByNode);
                constrainedSerialized = mapper.writeValueAsString(queryRoot);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Unable to modify serialized query to include the constraint column, cause: " + e.getMessage(), e);
            }
        }
        return constrainedSerialized;
    }

    @Override
    public String getMaxConstraintColumnValue(long serverTimestamp) {
        String previousMaxConstraintColumnValue = null;
        try {
            String maxValueQuery = buildMaxValueSerializedQuery();
            logger.info("Query for max constraint value: " + maxValueQuery);
            RowManager rowMgr = databaseClient.newRowManager();
            RawPlanDefinition maxConstraintValueQuery = rowMgr.newRawPlanDefinition(new StringHandle(maxValueQuery));
            JacksonHandle handle = new JacksonHandle().withFormat(Format.JSON).withMimetype("application/json");
            handle.setPointInTimeQueryTimestamp(serverTimestamp);
            JacksonHandle result = rowMgr.resultDoc(maxConstraintValueQuery, handle);
            String rawMaxConstraintColumnValue = result.get().get("rows").get(0).get("constraint").get("value").asText();
            previousMaxConstraintColumnValue = QueryHandlerUtil.sanitize(rawMaxConstraintColumnValue);
        } catch (Exception ex) {
            logger.warn("Failed to get a valid Maximum Constraint value: " + ex.getMessage());
        }
        return previousMaxConstraintColumnValue;
    }

    private String buildMaxValueSerializedQuery() throws JsonProcessingException {
        ObjectNode orderByDescendingNode = buildOrderByNode(false);
        ObjectNode limitOneNode = buildLimitNode(1);
        ObjectNode constraintColumnNode = buildMaxValueQueryConstraintNode();

        ObjectNode queryRoot = (ObjectNode) mapper.readTree(currentSerializedQuery);
        ArrayNode rootArgsArray = (ArrayNode) queryRoot.get("$optic").get("args");
        rootArgsArray.add(orderByDescendingNode);
        rootArgsArray.add(limitOneNode);
        rootArgsArray.add(constraintColumnNode);
        return mapper.writeValueAsString(queryRoot);
    }

    private ObjectNode buildMaxValueQueryConstraintNode() {
        return mapper.createObjectNode()
            .put("ns", "op")
            .put("fn", "select")
            .set("args", mapper.createArrayNode()
                .add(mapper.createArrayNode()
                    .add(mapper.createObjectNode()
                        .put("ns", "op")
                        .put("fn", "as")
                        .set("args", mapper.createArrayNode()
                            .add("constraint")
                            .add(mapper.createObjectNode()
                                .put("ns", "op")
                                .put("fn", "col")
                                .set("args", mapper.createArrayNode().add(constraintColumnName))
                            )
                        )
                )
            ));
    }

    private ObjectNode buildLimitNode(Integer limit) {
        return mapper.createObjectNode()
            .put("ns", "op")
            .put("fn", "limit")
            .set("args", mapper.createArrayNode().add(limit));
    }

    private ObjectNode buildOrderByNode(boolean ascending) {
        return mapper.createObjectNode()
            .put("ns", "op")
            .put("fn", "order-by")
            .set("args", mapper.createArrayNode()
                .add(mapper.createObjectNode()
                    .put("ns", "op")
                    .put("fn", ascending ? "asc" : "desc")
                    .set("args", mapper.createArrayNode().add(constraintColumnName))
            ));
    }

    private ObjectNode buildConstraintNode(String previousMaxConstraintColumnValue) {
        return mapper.createObjectNode()
            .put("ns", "op")
            .put("fn", "where")
            .set("args", mapper.createArrayNode()
                .add(mapper.createObjectNode()
                    .put("ns", "op")
                    .put("fn", "gt")
                    .set("args", mapper.createArrayNode()
                        .add(mapper.createObjectNode()
                            .put("ns", "op")
                            .put("fn", "col")
                            .set("args", mapper.createArrayNode().add(constraintColumnName))
                        )
                        .add(previousMaxConstraintColumnValue)
                    )
            ));
    }

    public String getCurrentQuery() {
        return currentSerializedQuery;
    }
}
