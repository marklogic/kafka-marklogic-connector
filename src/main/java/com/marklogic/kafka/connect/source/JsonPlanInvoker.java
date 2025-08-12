/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.JacksonHandle;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class JsonPlanInvoker extends AbstractPlanInvoker implements PlanInvoker {

    public JsonPlanInvoker(DatabaseClient client, Map<String, Object> parsedConfig) {
        super(client, parsedConfig);
    }

    @Override
    public Results invokePlan(PlanBuilder.Plan plan, String topic) {
        JacksonHandle baseHandle = new JacksonHandle();
        JacksonHandle result = newRowManager().resultDoc(plan, baseHandle);
        List<SourceRecord> records = new ArrayList<>();
        JsonNode doc = result.get();
        if (doc != null && doc.has("rows")) {
            for (JsonNode row : doc.get("rows")) {
                records.add(new SourceRecord(null, null, topic, null, getKeyValueFromRow(row), null, row.toString()));
            }
        }
        return new Results(records, baseHandle.getServerTimestamp());
    }

    private String getKeyValueFromRow(JsonNode row) {
        if (StringUtils.hasText(keyColumn) && row.has(keyColumn)) {
            JsonNode column = row.get(keyColumn);
            return this.includeColumnTypes ? column.get("value").asText() : column.asText();
        }
        return null;
    }
}
