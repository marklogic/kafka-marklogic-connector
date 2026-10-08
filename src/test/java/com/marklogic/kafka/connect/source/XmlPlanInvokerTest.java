/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.DOMHandle;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XmlPlanInvokerTest {

    private static final String ROWS_XML =
        "<t:table xmlns:t=\"http://marklogic.com/table\">" +
            "<t:rows>" +
            "<t:row><t:column name=\"ID\">42</t:column><t:column name=\"name\">Ada</t:column></t:row>" +
            "<t:row><t:column name=\"ID\">43</t:column><t:column name=\"name\">Grace</t:column></t:row>" +
            "</t:rows>" +
            "</t:table>";

    @Test
    void returnsNoRecordsWhenResponseHasNoDocument() {
        DatabaseClient client = StubRowManagerClient.returningResultDoc(handle -> {
        });

        PlanInvoker.Results results = new XmlPlanInvoker(client, new HashMap<>()).invokePlan(null, "topic");

        assertEquals(0, results.getSourceRecords().size());
    }

    @Test
    void convertsEachRowToASourceRecordKeyedByTheConfiguredColumn() throws Exception {
        List<SourceRecord> records = invoke(ROWS_XML, config("ID")).getSourceRecords();

        assertEquals(2, records.size());
        assertEquals("topic", records.get(0).topic());
        assertEquals("42", records.get(0).key());
        assertEquals("43", records.get(1).key());
        String value = (String) records.get(0).value();
        assertTrue(value.startsWith("<t:row"), "Each record holds the serialized row element; actual: " + value);
        assertTrue(value.contains("Ada"));
        assertTrue(!value.contains("<?xml"), "The XML declaration is omitted so rows can be embedded downstream");
    }

    @Test
    void returnsNullKeysWhenNoKeyColumnIsConfigured() throws Exception {
        List<SourceRecord> records = invoke(ROWS_XML, new HashMap<>()).getSourceRecords();

        assertEquals(2, records.size());
        assertNull(records.get(0).key());
    }

    @Test
    void skipsNodesThatHaveNoNameAttribute() throws Exception {
        String xml = "<t:table xmlns:t=\"http://marklogic.com/table\"><t:rows><t:row>" +
            "<t:column>no name attribute</t:column>" +
            "<t:column name=\"ID\">42</t:column>" +
            "</t:row></t:rows></t:table>";

        List<SourceRecord> records = invoke(xml, config("ID")).getSourceRecords();

        assertEquals("42", records.get(0).key());
    }

    @Test
    void returnsNullKeyWhenNoColumnMatchesTheKeyColumn() throws Exception {
        List<SourceRecord> records = invoke(ROWS_XML, config("doesnt-exist")).getSourceRecords();

        assertNull(records.get(0).key());
    }

    private PlanInvoker.Results invoke(String xml, Map<String, Object> parsedConfig) throws Exception {
        Document document = parse(xml);
        DatabaseClient client = StubRowManagerClient.returningResultDoc(handle -> ((DOMHandle) handle).set(document));
        return new XmlPlanInvoker(client, parsedConfig).invokePlan(null, "topic");
    }

    private Map<String, Object> config(String keyColumn) {
        Map<String, Object> parsedConfig = new HashMap<>();
        parsedConfig.put(MarkLogicSourceConfig.KEY_COLUMN, keyColumn);
        return parsedConfig;
    }

    private Document parse(String xml) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        disallowDoctypes(factory);
        return factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
    }

    private void disallowDoctypes(DocumentBuilderFactory factory) throws ParserConfigurationException {
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setExpandEntityReferences(false);
    }
}
