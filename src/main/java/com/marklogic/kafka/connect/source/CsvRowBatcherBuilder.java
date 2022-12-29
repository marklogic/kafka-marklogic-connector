package com.marklogic.kafka.connect.source;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.RowBatchSuccessListener;
import com.marklogic.client.datamovement.RowBatcher;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.ContentHandle;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

public class CsvRowBatcherBuilder extends AbstractRowBatcherBuilder<String> {

    CsvRowBatcherBuilder(DataMovementManager dataMovementManager, Map<String, Object> parsedConfig) {
        super(dataMovementManager, parsedConfig);
    }

    public RowBatcher<String> newRowBatcher(List<SourceRecord> newSourceRecords) {
        ContentHandle<String> contentHandle = new StringHandle().withFormat(Format.TEXT).withMimetype("text/csv");
        RowBatcher<String> rowBatcher =  dataMovementManager.newRowBatcher(contentHandle);
        configureRowBatcher(parsedConfig, rowBatcher);
        rowBatcher.onSuccess(event -> onSuccessHandler(event, newSourceRecords));
        return rowBatcher;
    }

    private void onSuccessHandler(RowBatchSuccessListener.RowBatchResponseEvent<String> event, List<SourceRecord> newSourceRecords) {
        String document = event.getRowsDoc();
        logger.debug("CSV document: \n" + document);
        BufferedReader reader = new BufferedReader(new StringReader(document));
        try {
            String headers = reader.readLine();
            reader.lines().forEach(line -> {
                String newDocument = headers + "\n" + line;
                SourceRecord newRecord = new SourceRecord(null, null, topic, null, newDocument);
                newSourceRecords.add(newRecord);
            });
        } catch (IOException ex) {
            logBatchError(ex);
        }
    }
}
