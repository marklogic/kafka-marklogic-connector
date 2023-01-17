package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.DOMHandle;
import org.apache.kafka.connect.source.SourceRecord;
import org.w3c.dom.Element;
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

    private DatabaseClient client;
    private Map<String, Object> parsedConfig;

    public XmlPlanInvoker(DatabaseClient client, Map<String, Object> parsedConfig) {
        this.client = client;
        this.parsedConfig = parsedConfig;
    }

    @Override
    public Results invokePlan(PlanBuilder.Plan plan, String topic) {
        DOMHandle baseHandle = new DOMHandle();
        DOMHandle result = client.newRowManager().resultDoc(plan, baseHandle);
        List<SourceRecord> records = result.get() != null ?
            convertRowsToSourceRecords(result, topic, baseHandle.getServerTimestamp()) :
            new ArrayList<>();
        return new Results(records, baseHandle.getServerTimestamp());
    }

    private List<SourceRecord> convertRowsToSourceRecords(DOMHandle result, String topic, long serverTimestamp) {
        Element docElement = result.get().getDocumentElement();
        NodeList rows = docElement.getElementsByTagNameNS(TABLE_NS_URI, "row");

        Transformer transformer;
        try {
            transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        } catch (Exception ex) {
            throw new RuntimeException("Unable to create XML transformer: " + ex.getMessage(), ex);
        }

        KeyGenerator keyGenerator = KeyGenerator.newKeyGenerator(this.parsedConfig, serverTimestamp);
        List<SourceRecord> records = new ArrayList<>();
        for (int i = 0; i < rows.getLength(); i++) {
            String value = documentToString(rows.item(i), transformer);
            records.add(new SourceRecord(null, null, topic, null, keyGenerator.generateKey(), null, value));
        }
        return records;
    }

    private String documentToString(Node newDoc, Transformer transformer) {
        try {
            StringWriter sw = new StringWriter();
            transformer.transform(new DOMSource(newDoc), new StreamResult(sw));
            return sw.toString();
        } catch (TransformerException ex) {
            throw new RuntimeException("Unable to transform XML to string: " + ex.getMessage(), ex);
        }
    }
}
