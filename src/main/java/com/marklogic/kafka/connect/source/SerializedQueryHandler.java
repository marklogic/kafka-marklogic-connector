package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.row.RowManager;
import com.marklogic.kafka.connect.MarkLogicConnectorException;
import org.springframework.util.StringUtils;

import java.util.Map;

public class SerializedQueryHandler extends LoggingObject implements QueryHandler {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String OPTIC_PLAN_ROOT_NODE = "$optic";

    private final DatabaseClient databaseClient;
    private final String constraintColumnName;
    private final Integer rowLimit;

    private final JsonNode currentSerializedQuery;

    public SerializedQueryHandler(DatabaseClient databaseClient, Map<String, Object> parsedConfig) {
        this.databaseClient = databaseClient;
        String userSerializedQuery = (String) parsedConfig.get(MarkLogicSourceConfig.SERIALIZED_QUERY);
        try {
            this.currentSerializedQuery = mapper.readTree(userSerializedQuery);
        } catch (JsonProcessingException e) {
            throw new MarkLogicConnectorException(
                String.format("Unable to read serialized query; cause: %s", e.getMessage()), e);
        }
        this.constraintColumnName = (String) parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME);
        rowLimit = (Integer) parsedConfig.get(MarkLogicSourceConfig.ROW_LIMIT);
    }

    @Override
    public PlanBuilder.Plan newPlan(String previousMaxConstraintColumnValue) {
        appendConstraintAndOrderByToQuery(currentSerializedQuery, previousMaxConstraintColumnValue);
        if (rowLimit > 0) {
            appendLimitToQuery(currentSerializedQuery);
        }
        logger.debug("Serialized query: {}", currentSerializedQuery);
        return databaseClient.newRowManager().newRawPlanDefinition(new JacksonHandle(currentSerializedQuery));
    }

    protected void appendLimitToQuery(JsonNode currentSerializedQuery) {
        ObjectNode limitNode = buildLimitNode(rowLimit);
        ArrayNode rootArgsArray = (ArrayNode) currentSerializedQuery.get(OPTIC_PLAN_ROOT_NODE).get("args");
        rootArgsArray.add(limitNode);
    }

    protected void appendConstraintAndOrderByToQuery(JsonNode currentSerializedQuery, String previousMaxConstraintColumnValue) {
        if (StringUtils.hasText(constraintColumnName)) {
            ObjectNode orderByNode = buildOrderByNode(true);
            ArrayNode rootArgsArray = (ArrayNode) currentSerializedQuery.get(OPTIC_PLAN_ROOT_NODE).get("args");
            if (StringUtils.hasText(previousMaxConstraintColumnValue)) {
                ObjectNode constraintNode = buildConstraintNode(previousMaxConstraintColumnValue);
                rootArgsArray.add(constraintNode);
            }
            rootArgsArray.add(orderByNode);
        }
    }

    @Override
    public String getMaxConstraintColumnValue(long serverTimestamp) {
        JsonNode maxValueQuery = buildMaxValueSerializedQuery();
        logger.debug("Query for max constraint value: {}", maxValueQuery);
        RowManager rowMgr = databaseClient.newRowManager();
        return QueryHandlerUtil.executeMaxValuePlan(rowMgr, rowMgr.newRawPlanDefinition(new JacksonHandle(maxValueQuery)),
            serverTimestamp, maxValueQuery.toString());
    }

    private JsonNode buildMaxValueSerializedQuery() {
        ObjectNode orderByDescendingNode = buildOrderByNode(false);
        ObjectNode limitOneNode = buildLimitNode(1);
        ObjectNode constraintColumnNode = buildMaxValueQueryConstraintNode();

        ArrayNode rootArgsArray = (ArrayNode) currentSerializedQuery.get(OPTIC_PLAN_ROOT_NODE).get("args");
        rootArgsArray.add(orderByDescendingNode);
        rootArgsArray.add(limitOneNode);
        rootArgsArray.add(constraintColumnNode);
        return currentSerializedQuery;
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
        return currentSerializedQuery.toString();
    }
}
