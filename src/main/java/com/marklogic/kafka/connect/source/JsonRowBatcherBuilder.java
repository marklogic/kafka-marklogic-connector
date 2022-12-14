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

public class JsonRowBatcherBuilder extends AbstractRowBatchBuilder implements RowBatcherBuilder<JsonNode> {

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

    protected void onSuccessHandler(RowBatchSuccessListener.RowBatchResponseEvent<JsonNode> event, List<SourceRecord> newSourceRecords) {
        JsonNode rows = event.getRowsDoc().get("rows");
        logger.debug("JsonNode: \n" + rows.toPrettyString());

        for (JsonNode row : rows) {
// We may need to add a switch to include a key in the record depending on how the target topic is configured.
// If the topic's cleanup policy is set to "compact", then a key is required to be included in the SourceRecord.
//            String key = event.getJobBatchNumber() + "-" + rowNumber;

            // Calling toString on the JsonNode, as when this is run in Confluent Platform, we get the following
            // error: org.apache.kafka.connect.errors.DataException: Java class com.fasterxml.jackson.databind.node.ObjectNode does not have corresponding schema type.
            try {
                SourceRecord newRecord = new SourceRecord(null, null, topic, null, row.toString());
                newSourceRecords.add(newRecord);
            } catch (Exception ex) {
                logBatchError(ex, row.toString());
            }
        }
    }
}
