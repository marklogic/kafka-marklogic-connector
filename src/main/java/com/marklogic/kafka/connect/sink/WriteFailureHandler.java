package com.marklogic.kafka.connect.sink;

import com.marklogic.client.datamovement.WriteBatch;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.datamovement.WriteFailureListener;
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;

/**
 * Handles a failed write batch, which currently just consists of logging information about the failed batch.
 */
public class WriteFailureHandler extends LoggingObject implements WriteFailureListener {

    private boolean includeKafkaMetadata;

    public WriteFailureHandler(boolean includeKafkaMetadata) {
        this.includeKafkaMetadata = includeKafkaMetadata;
    }

    @Override
    public void processFailure(WriteBatch batch, Throwable throwable) {
        logger.error("Batch failed; size: {}; cause: {}", batch.getItems().length, throwable.getMessage());
        if (this.includeKafkaMetadata) {
            logger.error("Logging Kafka record metadata for each failed document");
            for (WriteEvent event : batch.getItems()) {
                DocumentMetadataWriteHandle writeHandle = event.getMetadata();
                if (writeHandle instanceof DocumentMetadataHandle) {
                    DocumentMetadataHandle metadata = (DocumentMetadataHandle) writeHandle;
                    DocumentMetadataHandle.DocumentMetadataValues values = metadata.getMetadataValues();
                    if (values != null) {
                        logger.error("URI: {}; key: {}; partition: {}; offset: {}; timestamp: {}; topic: {}",
                            event.getTargetUri(), values.get("kafka-key"), values.get("kafka-partition"),
                            values.get("kafka-offset"), values.get("kafka-timestamp"), values.get("kafka-topic"));
                    }
                }
            }
        }
    }
}
