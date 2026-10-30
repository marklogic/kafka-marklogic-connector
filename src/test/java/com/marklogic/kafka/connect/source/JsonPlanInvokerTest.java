/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.JacksonHandle;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class JsonPlanInvokerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void returnsNoRecordsWhenResponseDoesNotContainRows() throws Exception {
        JsonPlanInvoker invoker = new JsonPlanInvoker(databaseClient(mapper.readTree("{}")), new HashMap<>());

        PlanInvoker.Results results = invoker.invokePlan(null, "topic");

        assertEquals(0, results.getSourceRecords().size());
    }

    @Test
    void readsRowsWithAndWithoutConfiguredKeyColumn() throws Exception {
        JsonNode response = mapper.readTree("{\"rows\":[{\"ID\":42,\"name\":\"Ada\"},{\"name\":\"Grace\"}]}");
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.KEY_COLUMN, "ID");
        JsonPlanInvoker invoker = new JsonPlanInvoker(databaseClient(response), config);

        List<SourceRecord> records = invoker.invokePlan(null, "authors").getSourceRecords();

        assertEquals(2, records.size());
        assertEquals("authors", records.get(0).topic());
        assertEquals("42", records.get(0).key());
        assertNull(records.get(1).key());
        assertEquals("Ada", mapper.readTree((String) records.get(0).value()).get("name").asText());
    }

    @Test
    void extractsKeyValueFromTypedColumn() throws Exception {
        JsonNode response = mapper.readTree("{\"rows\":[{\"ID\":{\"value\":42,\"type\":\"xs:integer\"}}]}");
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.KEY_COLUMN, "ID");
        config.put(MarkLogicSourceConfig.INCLUDE_COLUMN_TYPES, true);
        JsonPlanInvoker invoker = new JsonPlanInvoker(databaseClient(response), config);

        SourceRecord record = invoker.invokePlan(null, "authors").getSourceRecords().get(0);

        assertEquals("42", record.key());
    }

    private DatabaseClient databaseClient(JsonNode response) {
        return StubRowManagerClient.returningResultDoc(handle -> ((JacksonHandle) handle).set(response));
    }
}
