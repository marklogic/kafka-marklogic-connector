package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.RowBatcher;
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
    }

    @Override
    public void addQueryToRowBatcher(RowBatcher<?> rowBatcher, String previousMaxConstraintColumnValue) {
        currentSerializedQuery = injectConstraintIntoQuery(previousMaxConstraintColumnValue);
        logger.info("Serialized query: " + currentSerializedQuery);
        RowManager rowMgr = rowBatcher.getRowManager();
        RawPlanDefinition query = rowMgr.newRawPlanDefinition(new StringHandle(currentSerializedQuery));
        rowBatcher.withBatchView(query);
    }

    protected String injectConstraintIntoQuery(String previousMaxConstraintColumnValue) {
        String constrainedSerialized = userSerializedQuery;
        if (previousMaxConstraintColumnValue != null) {
            String constraintNodeString = "{\"ns\":\"op\", \"fn\":\"where\", \"args\":[{\"ns\":\"op\", \"fn\":\"gt\", \"args\":[{\"ns\":\"op\", \"fn\":\"col\", \"args\":[\"" + constraintColumnName + "\"]}, \"" + previousMaxConstraintColumnValue + "\"]}]}";
            try {
                ObjectNode constraintNode = (ObjectNode) mapper.readTree(constraintNodeString);

                ObjectNode queryRoot = (ObjectNode) mapper.readTree(userSerializedQuery);
                ArrayNode rootArgsArray = (ArrayNode) queryRoot.get("$optic").get("args");

                rootArgsArray.insert(1, constraintNode);
                constrainedSerialized = mapper.writeValueAsString(queryRoot);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Unable to modify serialized query to include the constraint column, cause: " + e.getMessage(), e);
            }
        }
        return constrainedSerialized;
    }

    @Override
    public String updatePreviousMaxConstraintColumnValue(long queryStartTimeInMillis) {
        String previousMaxConstraintColumnValue = null;
        try {
            String maxValueQuery = buildMaxValueSerializedQuery();
            logger.info("Query for max constraint value: " + maxValueQuery);
            RowManager rowMgr = databaseClient.newRowManager();
            RawPlanDefinition maxConstraintValueQuery = rowMgr.newRawPlanDefinition(new StringHandle(maxValueQuery));
            JacksonHandle handle = new JacksonHandle().withFormat(Format.JSON).withMimetype("application/json");
            handle.setPointInTimeQueryTimestamp(queryStartTimeInMillis);
            JacksonHandle result = rowMgr.resultDoc(maxConstraintValueQuery, handle);
            String rawMaxConstraintColumnValue = result.get().get("rows").get(0).get("constraint").get("value").asText();
            previousMaxConstraintColumnValue = QueryHandlerUtil.sanitize(rawMaxConstraintColumnValue);
        } catch (Exception ex) {
            logger.warn("Failed to get a valid Maximum Constraint value: " + ex.getMessage());
        }
        return previousMaxConstraintColumnValue;
    }

    private String buildMaxValueSerializedQuery() throws JsonProcessingException {
        String orderByDescendingPhrase = "{\"ns\":\"op\", \"fn\":\"order-by\", \"args\":[{\"ns\":\"op\", \"fn\":\"desc\", \"args\":[\"" + constraintColumnName + "\"]}]}";
        ObjectNode orderByDescendingNode = (ObjectNode) mapper.readTree(orderByDescendingPhrase);
        String limitOnePhase = "{\"ns\":\"op\", \"fn\":\"limit\", \"args\":[1]}";
        ObjectNode limitOneNode = (ObjectNode) mapper.readTree(limitOnePhase);
        String constraintColumnPhase = "{\"ns\":\"op\", \"fn\":\"select\", \"args\":[[{\"ns\":\"op\", \"fn\":\"as\", \"args\":[\"constraint\", {\"ns\":\"op\", \"fn\":\"col\", \"args\":[\"" + constraintColumnName + "\"]}]}]]}";
        ObjectNode constraintColumnNode = (ObjectNode) mapper.readTree(constraintColumnPhase);
        ObjectNode queryRoot = (ObjectNode) mapper.readTree(currentSerializedQuery);
        ArrayNode rootArgsArray = (ArrayNode) queryRoot.get("$optic").get("args");
        rootArgsArray.add(orderByDescendingNode);
        rootArgsArray.add(limitOneNode);
        rootArgsArray.add(constraintColumnNode);
        return mapper.writeValueAsString(queryRoot);
    }
}
