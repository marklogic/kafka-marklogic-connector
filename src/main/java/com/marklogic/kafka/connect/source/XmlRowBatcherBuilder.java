package com.marklogic.kafka.connect.source;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.RowBatchSuccessListener;
import com.marklogic.client.datamovement.RowBatcher;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.marker.ContentHandle;
import org.apache.kafka.connect.source.SourceRecord;
import org.w3c.dom.Document;
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
import java.util.List;
import java.util.Map;

public class XmlRowBatcherBuilder extends AbstractRowBatcherBuilder<Document> {

    private static final String TABLE_NS_URI = "http://marklogic.com/table";

    // While a Transformer is not thread-safe and must therefore be created for each batch - though we could consider
    // a pooling strategy in the future - a TransformerFactory is thread-safe and can thus be reused
    private static final TransformerFactory transformerFactory = TransformerFactory.newInstance();

    XmlRowBatcherBuilder(DataMovementManager dataMovementManager, Map<String, Object> parsedConfig) {
        super(dataMovementManager, parsedConfig);
    }

    public RowBatcher<Document> newRowBatcher(List<SourceRecord> newSourceRecords) {
        ContentHandle<Document> domHandle = new DOMHandle().withMimetype("application/xml");
        RowBatcher<Document> rowBatcher = dataMovementManager.newRowBatcher(domHandle);
        configureRowBatcher(parsedConfig, rowBatcher);
        rowBatcher.onSuccess(event -> onSuccessHandler(event, newSourceRecords));
        return rowBatcher;
    }

    private void onSuccessHandler(RowBatchSuccessListener.RowBatchResponseEvent<Document> event, List<SourceRecord> newSourceRecords) {
        NodeList rows = extractRowsFromResponse(event);

        Transformer transformer;
        try {
            transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        } catch (Exception ex) {
            logBatchError(ex);
            return;
        }

        for (int i = 0; i < rows.getLength(); i++) {
            try {
                SourceRecord newRecord = new SourceRecord(null, null, topic, null, documentToString(rows.item(i), transformer));
                newSourceRecords.add(newRecord);
            } catch (Exception ex) {
                try {
                    logBatchError(ex, documentToString(rows.item(i), transformer));
                } catch (TransformerException e) {
                    logBatchError(ex);
                }
            }
        }
    }

    private NodeList extractRowsFromResponse(RowBatchSuccessListener.RowBatchResponseEvent<Document> event) {
        Document rowsDoc = event.getRowsDoc();
        Element docElement = rowsDoc.getDocumentElement();
        return docElement.getElementsByTagNameNS(TABLE_NS_URI, "row");
    }

    private String documentToString(Node newDoc, Transformer transformer) throws TransformerException {
        StringWriter sw = new StringWriter();
        transformer.transform(new DOMSource(newDoc), new StreamResult(sw));
        return sw.toString();
    }
}
