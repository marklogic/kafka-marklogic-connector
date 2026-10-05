/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.StringHandle;
import com.marklogic.kafka.connect.MarkLogicConnectorException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvPlanInvokerTest {

    private static final String CSV = "id,name\n1,Ada\n2,Grace";

    @Test
    void returnsNoRecordsWhenResultIsEmpty() {
        DatabaseClient client = StubRowManagerClient.returningResultDoc(handle -> {
        });

        PlanInvoker.Results results = new CsvPlanInvoker(client, new HashMap<>()).invokePlan(null, "topic");

        assertEquals(0, results.getSourceRecords().size());
    }

    @Test
    void usesConfiguredKeyColumnAndRepeatsHeadersOnEachRecord() {
        List<SourceRecord> records = invoke(config("name")).getSourceRecords();

        assertEquals(2, records.size());
        assertEquals("topic", records.get(0).topic());
        assertEquals("Ada", records.get(0).key());
        assertEquals("Grace", records.get(1).key());
        assertEquals("id,name\n1,Ada", records.get(0).value());
        assertEquals("id,name\n2,Grace", records.get(1).value());
    }

    @Test
    void returnsNullKeysWhenNoKeyColumnIsConfigured() {
        List<SourceRecord> records = invoke(new HashMap<>()).getSourceRecords();

        assertEquals(2, records.size());
        assertNull(records.get(0).key());
        assertNull(records.get(1).key());
    }

    @Test
    void returnsNullKeysWhenKeyColumnIsNotInTheHeaders() {
        List<SourceRecord> records = invoke(config("doesnt-exist")).getSourceRecords();

        assertEquals(2, records.size());
        assertNull(records.get(0).key());
    }

    @Test
    void includeColumnTypesIsIgnoredForCsv() {
        Map<String, Object> parsedConfig = config("id");
        parsedConfig.put(MarkLogicSourceConfig.INCLUDE_COLUMN_TYPES, true);

        List<SourceRecord> records = invoke(parsedConfig).getSourceRecords();

        assertEquals("1", records.get(0).key(), "The option is only expected to produce a warning for CSV");
    }

    @Test
    void failsWithAHelpfulMessageWhenTheHeaderLineCannotBeParsed() {
        MarkLogicConnectorException error = assertThrows(MarkLogicConnectorException.class,
            () -> invokeWith("id,\"na\"me\n1,Ada", config("name")));

        assertTrue(error.getMessage().startsWith("Unable to parse CSV; line: "), error.getMessage());
    }

    @Test
    void failsWithAHelpfulMessageWhenARowCannotBeParsed() {
        MarkLogicConnectorException error = assertThrows(MarkLogicConnectorException.class,
            () -> invokeWith("id,name\n1,\"Ad\"a", config("name")));

        assertTrue(error.getMessage().startsWith("Unable to read CSV; line: "), error.getMessage());
    }

    private PlanInvoker.Results invoke(Map<String, Object> parsedConfig) {
        return invokeWith(CSV, parsedConfig);
    }

    private PlanInvoker.Results invokeWith(String csv, Map<String, Object> parsedConfig) {
        DatabaseClient client = StubRowManagerClient.returningResultDoc(handle -> ((StringHandle) handle).set(csv));
        return new CsvPlanInvoker(client, parsedConfig).invokePlan(null, "topic");
    }

    private Map<String, Object> config(String keyColumn) {
        Map<String, Object> parsedConfig = new HashMap<>();
        parsedConfig.put(MarkLogicSourceConfig.KEY_COLUMN, keyColumn);
        return parsedConfig;
    }
}
