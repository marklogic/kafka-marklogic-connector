/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.kafka.connect.MarkLogicConnectorException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class XmlPlanInvoker extends AbstractPlanInvoker implements PlanInvoker {

    private static final Logger staticLogger = LoggerFactory.getLogger(XmlPlanInvoker.class);

    private static final String TABLE_NS_URI = "http://marklogic.com/table";

    // While a Transformer is not thread-safe and must therefore be created for each batch - though we could consider
    // a pooling strategy in the future - a TransformerFactory is thread-safe and can thus be reused
    private static final TransformerFactory transformerFactory = makeNewTransformerFactory();

    public XmlPlanInvoker(DatabaseClient client, Map<String, Object> parsedConfig) {
        super(client, parsedConfig);
    }

    @Override
    public Results invokePlan(PlanBuilder.Plan plan, String topic) {
        DOMHandle baseHandle = new DOMHandle();
        DOMHandle result = newRowManager().resultDoc(plan, baseHandle);
        List<SourceRecord> records = result.get() != null ?
            convertRowsToSourceRecords(result, topic) :
            new ArrayList<>();
        return new Results(records, baseHandle.getServerTimestamp());
    }

    private List<SourceRecord> convertRowsToSourceRecords(DOMHandle result, String topic) {
        Element docElement = result.get().getDocumentElement();
        NodeList rows = docElement.getElementsByTagNameNS(TABLE_NS_URI, "row");

        Transformer transformer;
        try {
            transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        } catch (Exception ex) {
            throw new MarkLogicConnectorException("Unable to create XML transformer: " + ex.getMessage(), ex);
        }

        List<SourceRecord> records = new ArrayList<>();
        for (int i = 0; i < rows.getLength(); i++) {
            Node row = rows.item(i);
            String value = documentToString(row, transformer);
            records.add(new SourceRecord(null, null, topic, null, getKeyFromRow(row), null, value));
        }
        return records;
    }

    private String getKeyFromRow(Node row) {
        if (StringUtils.hasText(keyColumn)) {
            NodeList columns = row.getChildNodes();
            int len = columns.getLength();
            for (int j = 0; j < len; j++) {
                Node column = columns.item(j);
                NamedNodeMap attributes = column.getAttributes();
                // The 'name' attribute is expected to exist; trust but verify
                if (attributes != null && attributes.getNamedItem("name") != null &&
                    keyColumn.equals(attributes.getNamedItem("name").getTextContent())) {
                    return column.getTextContent();
                }
            }
        }
        return null;
    }

    private String documentToString(Node newDoc, Transformer transformer) {
        try {
            StringWriter sw = new StringWriter();
            transformer.transform(new DOMSource(newDoc), new StreamResult(sw));
            return sw.toString();
        } catch (TransformerException ex) {
            throw new MarkLogicConnectorException("Unable to transform XML to string: " + ex.getMessage(), ex);
        }
    }

    private static TransformerFactory makeNewTransformerFactory() {
        TransformerFactory factory = TransformerFactory.newInstance();
        // Avoids Polaris warning related to
        // https://cwe.mitre.org/data/definitions/611.html .
        // From
        // https://stackoverflow.com/questions/32178558/how-to-prevent-xml-external-entity-injection-on-transformerfactory
        // .
        try {
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        } catch (TransformerConfigurationException e) {
            logTransformerFactoryWarning(XMLConstants.FEATURE_SECURE_PROCESSING, e.getMessage());
        }
        try {
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        } catch (IllegalArgumentException e) {
            logTransformerFactoryWarning(XMLConstants.ACCESS_EXTERNAL_DTD, e.getMessage());
        }
        try {
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
        } catch (IllegalArgumentException e) {
            logTransformerFactoryWarning(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, e.getMessage());
        }
        return factory;
    }

    private static void logTransformerFactoryWarning(String xmlConstant, String errorMessage) {
        String baseTransformerFactoryWarningMessage = "Unable to set {} on TransformerFactory; cause: {}";
        staticLogger.warn(baseTransformerFactoryWarningMessage, xmlConstant, errorMessage);
    }

}
