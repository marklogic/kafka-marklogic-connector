package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.row.RowManager;
import com.marklogic.kafka.connect.MarkLogicConnectorException;
import org.apache.kafka.connect.source.SourceRecord;
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

class XmlPlanInvoker implements PlanInvoker {

    private static final String TABLE_NS_URI = "http://marklogic.com/table";

    // While a Transformer is not thread-safe and must therefore be created for each batch - though we could consider
    // a pooling strategy in the future - a TransformerFactory is thread-safe and can thus be reused
    private static final TransformerFactory transformerFactory = TransformerFactory.newInstance();

    private final DatabaseClient client;
    private final String keyColumn;

    public XmlPlanInvoker(DatabaseClient client, Map<String, Object> parsedConfig) {
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
        DOMHandle baseHandle = new DOMHandle();
        RowManager rowManager = client.newRowManager();
        rowManager.setDatatypeStyle(RowManager.RowSetPart.HEADER);
        DOMHandle result = rowManager.resultDoc(plan, baseHandle);
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

        System.out.println("DOC: " + documentToString(result.get(), transformer));

        List<SourceRecord> records = new ArrayList<>();
        for (int i = 0; i < rows.getLength(); i++) {
            Node row = rows.item(i);
            String value = documentToString(row, transformer);
            System.out.println("DOC: " + value);
            records.add(new SourceRecord(null, null, topic, null, getKeyFromRow(row), null, value));
        }
        return records;
    }

    private String getKeyFromRow(Node row) {
        if (keyColumn != null) {
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
