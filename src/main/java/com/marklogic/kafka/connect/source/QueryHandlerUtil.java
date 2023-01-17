package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.row.RowManager;
import com.marklogic.kafka.connect.MarkLogicConnectorException;

interface QueryHandlerUtil {

    /**
     * Convenience method for executing a plan to get the maximum value for a particular constraint column; exists
     * primarily to provide consistent error handling in case the query fails.
     *
     * @param rowManager
     * @param maxValuePlan    the plan for getting a maximum value; the assumption is this will return a column named 'constraint'
     *                        that contains the value
     * @param serverTimestamp the MarkLogic server timestamp at which to run the plan's query
     * @param maxValueQuery   a human-readable representation of the query; used solely for error handling
     * @return
     */
    static String executeMaxValuePlan(RowManager rowManager, PlanBuilder.Plan maxValuePlan, long serverTimestamp, String maxValueQuery) {
        JacksonHandle handle = new JacksonHandle();
        handle.setPointInTimeQueryTimestamp(serverTimestamp);
        JacksonHandle result = rowManager.resultDoc(maxValuePlan, handle);
        JsonNode valueNode = result.get().at("/rows/0/constraint/value");
        if (valueNode == null) {
            String message = String.format(
                "Unable to get max constraint value; query returned null; query: %s; server timestamp: %d; response: %s",
                maxValueQuery, serverTimestamp, result.get());
            throw new MarkLogicConnectorException(message);
        }
        return valueNode.asText();
    }
}
