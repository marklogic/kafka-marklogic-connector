package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.row.RowManager;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class JsonPlanInvoker implements PlanInvoker {

    private final DatabaseClient client;
    private final String keyColumn;

    public JsonPlanInvoker(DatabaseClient client, Map<String, Object> parsedConfig) {
        this.client = client;
        String value = (String) parsedConfig.get(MarkLogicSourceConfig.KEY_COLUMN);
        if (value != null && value.trim().length() > 0) {
            this.keyColumn = value;
        } else {
            this.keyColumn = null;
        }
    }

    @Override
    public Results invokePlan(PlanBuilder.Plan plan, String topic) {
        JacksonHandle baseHandle = new JacksonHandle();
        RowManager mgr = client.newRowManager();
        mgr.setDatatypeStyle(RowManager.RowSetPart.HEADER);
        JacksonHandle result = mgr.resultDoc(plan, baseHandle);
        List<SourceRecord> records = new ArrayList<>();
        /**
         * Testing has shown that converting the JSON to a string and using
         * org.apache.kafka.connect.storage.StringConverter as the value converter works well. And a user could
         * still choose to use org.apache.kafka.connect.json.JsonConverter, though they would most likely want
         * to set "value.converter.schemas.enable" to "false".
         */
        JsonNode doc = result.get();
        if (doc != null && doc.has("rows")) {
            System.out.println("ROW: " + doc.toPrettyString());
            for (JsonNode row : doc.get("rows")) {
                records.add(new SourceRecord(null, null, topic, null, getKeyValueFromRow(row), null, row.toString()));
            }
        }
        return new Results(records, baseHandle.getServerTimestamp());
    }

    private String getKeyValueFromRow(JsonNode row) {
        if (keyColumn != null && row.has(keyColumn)) {
            JsonNode column = row.get(keyColumn);
            // The "value" child is expected to exist; trust but verify
            return column.has("value") ? column.get("value").asText() : null;
        }
        return null;
    }
}
