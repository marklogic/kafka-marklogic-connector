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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.kafka.connect.MarkLogicConnectorException;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class XmlPlanInvoker extends AbstractPlanInvoker implements PlanInvoker {

    private static final String TABLE_NS_URI = "http://marklogic.com/table";

    // While a Transformer is not thread-safe and must therefore be created for each batch - though we could consider
    // a pooling strategy in the future - a TransformerFactory is thread-safe and can thus be reused
    private static final TransformerFactory transformerFactory = TransformerFactory.newInstance();

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
}
