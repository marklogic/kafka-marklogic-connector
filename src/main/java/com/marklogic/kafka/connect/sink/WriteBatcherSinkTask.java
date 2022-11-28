package com.marklogic.kafka.connect.sink;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.*;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;
import com.marklogic.kafka.connect.ConfigUtil;
import com.marklogic.kafka.connect.DefaultDatabaseClientConfigBuilder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Uses MarkLogic's Data Movement SDK (DMSDK) to write data to MarkLogic.
 */
public class WriteBatcherSinkTask extends AbstractSinkTask {

    private DatabaseClient databaseClient;
    private DataMovementManager dataMovementManager;
    private WriteBatcher writeBatcher;
    private SinkRecordConverter sinkRecordConverter;

    @Override
    protected void onStart(Map<String, Object> parsedConfig) {
        DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder().buildDatabaseClientConfig(parsedConfig);
        this.databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);

        dataMovementManager = databaseClient.newDataMovementManager();
        writeBatcher = dataMovementManager.newWriteBatcher();
        configureWriteBatcher(parsedConfig, writeBatcher);

        final String flowName = (String) parsedConfig.get(MarkLogicSinkConfig.DATAHUB_FLOW_NAME);
        if (flowName != null && flowName.trim().length() > 0) {
            writeBatcher.onBatchSuccess(buildRunFlowListener(flowName, parsedConfig, databaseClientConfig));
        }

        dataMovementManager.startJob(writeBatcher);

        this.sinkRecordConverter = new DefaultSinkRecordConverter(parsedConfig);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        super.put(records);
        // An async flush can be performed here since Kafka expects any writes to be async within this method
        this.writeBatcher.flushAsync();
    }

    @Override
    protected void writeSinkRecord(SinkRecord sinkRecord) {
        this.writeBatcher.add(this.sinkRecordConverter.convert(sinkRecord));
    }

    /**
     * Because each call to {@code put} results in the records being flushed asynchronously, it is not expected that
     * anything will need to be flushed here. But just in case, {@code flushAndWait} is used here to ensure that a
     * user expection of all data being flushed is met.
     *
     * @param currentOffsets
     */
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        if (writeBatcher != null) {
            writeBatcher.flushAndWait();
        }
    }

    @Override
    public void stop() {
        if (writeBatcher != null) {
            writeBatcher.flushAndWait();
            dataMovementManager.stopJob(writeBatcher);
        }
        if (databaseClient != null) {
            databaseClient.release();
        }
    }

    /**
     * Configure the given WriteBatcher based on DMSDK-related options in the parsedConfig.
     *
     * @param parsedConfig
     * @param writeBatcher
     */
    private void configureWriteBatcher(Map<String, Object> parsedConfig, WriteBatcher writeBatcher) {
        Integer batchSize = (Integer) parsedConfig.get(MarkLogicSinkConfig.DMSDK_BATCH_SIZE);
        if (batchSize != null) {
            logger.info("DMSDK batch size: " + batchSize);
            writeBatcher.withBatchSize(batchSize);
        }

        Integer threadCount = (Integer) parsedConfig.get(MarkLogicSinkConfig.DMSDK_THREAD_COUNT);
        if (threadCount != null) {
            logger.info("DMSDK thread count: " + threadCount);
            writeBatcher.withThreadCount(threadCount);
        }

        ServerTransform transform = buildServerTransform(parsedConfig);
        if (transform != null) {
            // Not logging transform parameters as they may contain sensitive values
            logger.info("Will apply server transform: " + transform.getName());
            writeBatcher.withTransform(transform);
        }

        String temporalCollection = (String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_TEMPORAL_COLLECTION);
        if (StringUtils.hasText(temporalCollection)) {
            logger.info("Will add documents to temporal collection: " + temporalCollection);
            writeBatcher.withTemporalCollection(temporalCollection);
        }

        writeBatcher.onBatchFailure(new WriteFailureHandler(
            ConfigUtil.getBoolean(MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_METADATA, parsedConfig)));
    }

    /**
     * This is all specific to Kafka, as it involves reading inputs from the Kafka config map and then using them to
     * construct the reusable RunFlowWriteBatchListener.
     *
     * @param flowName
     * @param parsedConfig
     * @param databaseClientConfig
     */
    protected RunFlowWriteBatchListener buildRunFlowListener(String flowName, Map<String, Object> parsedConfig,
                                                             DatabaseClientConfig databaseClientConfig) {
        String logMessage = String.format("After ingesting a batch, will run flow '%s'", flowName);
        final String flowSteps = (String) parsedConfig.get(MarkLogicSinkConfig.DATAHUB_FLOW_STEPS);
        List<String> steps = null;
        if (flowSteps != null && flowSteps.trim().length() > 0) {
            steps = Arrays.asList(flowSteps.split(","));
            logMessage += String.format(" with steps '%s' constrained to the URIs in that batch", steps);
        }
        logger.info(logMessage);

        RunFlowWriteBatchListener listener = new RunFlowWriteBatchListener(flowName, steps, databaseClientConfig);
        if (parsedConfig.get(MarkLogicSinkConfig.DATAHUB_FLOW_LOG_RESPONSE) != null) {
            listener.setLogResponse(ConfigUtil.getBoolean(MarkLogicSinkConfig.DATAHUB_FLOW_LOG_RESPONSE, parsedConfig));
        }
        return listener;
    }

    /**
     * Builds a REST ServerTransform object based on the DMSDK parameters in the given config. If no transform name
     * is configured, then null will be returned.
     *
     * @param parsedConfig - The complete configuration object including any transform parameters.
     * @return - The ServerTransform that will operate on each record, or null
     */
    protected ServerTransform buildServerTransform(final Map<String, Object> parsedConfig) {
        String transform = (String) parsedConfig.get(MarkLogicSinkConfig.DMSDK_TRANSFORM);
        if (StringUtils.hasText(transform)) {
            ServerTransform t = new ServerTransform(transform);
            String params = (String) parsedConfig.get(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS);
            if (params != null && params.trim().length() > 0) {
                String delimiter = (String) parsedConfig.get(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS_DELIMITER);
                if (delimiter != null && delimiter.trim().length() > 0) {
                    String[] tokens = params.split(delimiter);
                    for (int i = 0; i < tokens.length; i += 2) {
                        if (i + 1 >= tokens.length) {
                            throw new IllegalArgumentException(String.format("The value of the %s property does not have an even number of " +
                                "parameter names and values; property value: %s", MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS, params));
                        }
                        t.addParameter(tokens[i], tokens[i + 1]);
                    }
                } else {
                    logger.warn(String.format("Unable to apply transform parameters to transform: %s; please set the " +
                        "delimiter via the %s property", transform, MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS_DELIMITER));
                }
            }
            return t;
        }
        return null;
    }

    /**
     * Exposed for testing.
     *
     * @return
     */
    protected WriteBatcher getWriteBatcher() {
        return this.writeBatcher;
    }
}

class WriteFailureHandler extends LoggingObject implements WriteFailureListener {

    private final boolean includeKafkaMetadata;

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
