package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.RowBatchSuccessListener;
import com.marklogic.client.datamovement.RowBatcher;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.ContentHandle;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;

public class JsonRowBatcherBuilder extends AbstractRowBatcherBuilder<JsonNode> {

    JsonRowBatcherBuilder(DataMovementManager dataMovementManager, Map<String, Object> parsedConfig) {
        super(dataMovementManager, parsedConfig);
    }

    public RowBatcher<JsonNode> newRowBatcher(List<SourceRecord> newSourceRecords) {
        ContentHandle<JsonNode> contentHandle = new JacksonHandle().withFormat(Format.JSON).withMimetype("application/json");
        RowBatcher<JsonNode> rowBatcher =  dataMovementManager.newRowBatcher(contentHandle);
        configureRowBatcher(parsedConfig, rowBatcher);
        rowBatcher.onSuccess(event -> onSuccessHandler(event, newSourceRecords));
        return rowBatcher;
    }

    private void onSuccessHandler(RowBatchSuccessListener.RowBatchResponseEvent<JsonNode> event, List<SourceRecord> newSourceRecords) {
        JsonNode rows = event.getRowsDoc().get("rows");
        for (JsonNode row : rows) {
            /**
             * Testing has shown that converting the JSON to a string and using
             * org.apache.kafka.connect.storage.StringConverter as the value converter works well. And a user could
             * still choose to use org.apache.kafka.connect.json.JsonConverter, though they would most likely want
             * to set "value.converter.schemas.enable" to "false".
             */
            try {
                SourceRecord newRecord = new SourceRecord(null, null, topic, null, row.toString());
                newSourceRecords.add(newRecord);
            } catch (Exception ex) {
                logBatchError(ex, row.toString());
            }
        }
    }
}
