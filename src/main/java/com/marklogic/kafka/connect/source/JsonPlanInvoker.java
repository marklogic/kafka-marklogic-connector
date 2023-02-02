/*
 * Copyright (c) 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
