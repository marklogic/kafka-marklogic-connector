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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.runtime.InternalSinkRecord;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import static com.marklogic.kafka.connect.sink.AbstractSinkTask.*;

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
        if (errorReporterMethod == null) {
            errorReporterMethod = getErrorReporter();
        }

        dataMovementManager = databaseClient.newDataMovementManager();
        writeBatcher = dataMovementManager.newWriteBatcher();
        configureWriteBatcher(parsedConfig, writeBatcher);

        final String flowName = (String) parsedConfig.get(MarkLogicSinkConfig.DATAHUB_FLOW_NAME);
        if (StringUtils.hasText(flowName)) {
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
        try {
            this.writeBatcher.add(this.sinkRecordConverter.convert(sinkRecord));
        } catch (Exception e) {
            logger.error("Unable to convert sink record into a document to be written to MarkLogic; " +
                    "record key: {}; cause: {}", sinkRecord.key(), e.getMessage());
            addFailureHeaders(sinkRecord, e, AbstractSinkTask.MARKLOGIC_CONVERSION_FAILURE, null);
            errorReporterMethod.accept(sinkRecord, e);
        }
    }

    static void addFailureHeaders(SinkRecord sinkRecord, Throwable e, String failureHeaderValue, WriteEvent writeEvent) {
        if (sinkRecord instanceof InternalSinkRecord) {
            ConsumerRecord<byte[], byte[]> originalRecord = ((InternalSinkRecord) sinkRecord).originalRecord();
            originalRecord.headers().add(MARKLOGIC_MESSAGE_FAILURE_HEADER, getBytesHandleNull(failureHeaderValue));
            originalRecord.headers().add(MARKLOGIC_MESSAGE_EXCEPTION_MESSAGE, getBytesHandleNull(e.getMessage()));
            originalRecord.headers().add(MARKLOGIC_ORIGINAL_TOPIC, getBytesHandleNull(sinkRecord.topic()));
            if (writeEvent != null) {
                originalRecord.headers().add(MARKLOGIC_TARGET_URI, writeEvent.getTargetUri().getBytes(StandardCharsets.UTF_8));
            }
        } else {
            sinkRecord.headers().addString(MARKLOGIC_MESSAGE_FAILURE_HEADER, failureHeaderValue);
            sinkRecord.headers().addString(MARKLOGIC_MESSAGE_EXCEPTION_MESSAGE, e.getMessage());
            sinkRecord.headers().addString(MARKLOGIC_ORIGINAL_TOPIC, sinkRecord.topic());
            if (writeEvent != null) {
                sinkRecord.headers().addString(MARKLOGIC_TARGET_URI, writeEvent.getTargetUri());
            }
        }
    }

    private static byte[] getBytesHandleNull(String value) {
        return (value != null) ? value.getBytes(StandardCharsets.UTF_8) : null;
    }

    /**
     * Because each call to {@code put} results in the records being flushed asynchronously, it is not expected that
     * anything will need to be flushed here. But just in case, {@code flushAndWait} is used here to ensure that a
     * user expection of all data being flushed is met.
     *
     * @param currentOffsets– the current offset state as of the last call to put(Collection)}, provided for
     *                        convenience but could also be determined by tracking all offsets included in the
     *                        SinkRecords passed to put.
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
            logger.info("DMSDK batch size: {}", batchSize);
            writeBatcher.withBatchSize(batchSize);
        }

        Integer threadCount = (Integer) parsedConfig.get(MarkLogicSinkConfig.DMSDK_THREAD_COUNT);
        if (threadCount != null) {
            logger.info("DMSDK thread count: {}", threadCount);
            writeBatcher.withThreadCount(threadCount);
        }

        Optional<ServerTransform> transform = buildServerTransform(parsedConfig);
        if (transform.isPresent()) {
            // Not logging transform parameters as they may contain sensitive values
            logger.info("Will apply server transform: {}", transform.get().getName());
            writeBatcher.withTransform(transform.get());
        }

        String temporalCollection = (String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_TEMPORAL_COLLECTION);
        if (StringUtils.hasText(temporalCollection)) {
            logger.info("Will add documents to temporal collection: {}", temporalCollection);
            writeBatcher.withTemporalCollection(temporalCollection);
        }

        writeBatcher.onBatchFailure(new WriteFailureHandler(
            ConfigUtil.getBoolean(MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_METADATA, parsedConfig), errorReporterMethod));
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
        if (StringUtils.hasText(flowSteps)) {
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
    protected Optional<ServerTransform> buildServerTransform(final Map<String, Object> parsedConfig) {
        String transformName = (String) parsedConfig.get(MarkLogicSinkConfig.DMSDK_TRANSFORM);
        if (StringUtils.hasText(transformName)) {
            ServerTransform transform = new ServerTransform(transformName);
            String params = (String) parsedConfig.get(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS);
            if (StringUtils.hasText(params)) {
                String delimiter = (String) parsedConfig.get(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS_DELIMITER);
                if (StringUtils.hasText(delimiter)) {
                    addTransformParameters(transform, params, delimiter);
                } else {
                    logger.warn("Unable to apply transform parameters to transform: {}; please set the " +
                        "delimiter via the {} property", transform, MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS_DELIMITER);
                }
            }
            return Optional.of(transform);
        }
        return Optional.empty();
    }

    private void addTransformParameters(ServerTransform transform, String params, String delimiter) {
        String[] tokens = params.split(delimiter);
        for (int i = 0; i < tokens.length; i += 2) {
            if (i + 1 >= tokens.length) {
                throw new IllegalArgumentException(String.format("The value of the %s property does not have an even number of " +
                    "parameter names and values; property value: %s", MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS, params));
            }
            transform.addParameter(tokens[i], tokens[i + 1]);
        }
    }

    /**
     * Exposed for testing.
     *
     * @return
     */
    protected WriteBatcher getWriteBatcher() {
        return this.writeBatcher;
    }
    protected void setSinkRecordConverter(SinkRecordConverter sinkRecordConverter) {
        this.sinkRecordConverter = sinkRecordConverter;
    }

    private BiConsumer<SinkRecord, Throwable> getErrorReporter() {
        BiConsumer<SinkRecord, Throwable> errorReporter = this::nopReport;
        if (context != null) {
            try {
                ErrantRecordReporter errantRecordReporter = context.errantRecordReporter();
                if (errantRecordReporter != null) {
                    errorReporter = errantRecordReporter::report;
                } else {
                    logger.info("Errant record reporter not configured.");
                }
            } catch (NoClassDefFoundError | NoSuchMethodError e) {
                // Will occur in Connect runtimes earlier than 2.6
                logger.info("Kafka versions prior to 2.6 do not support the errant record reporter.");
            }
        }
        return errorReporter;
    }

    private void nopReport(SinkRecord sinkRecord, Throwable e) {
        // Used when errors cannot be sent to DLQ because Kafka version is too old
    }
}

class WriteFailureHandler extends LoggingObject implements WriteFailureListener {

    private final boolean includeKafkaMetadata;
    private final BiConsumer<SinkRecord, Throwable> errorReporterMethod;

    public WriteFailureHandler(boolean includeKafkaMetadata, BiConsumer<SinkRecord, Throwable> errorReporterMethod) {
        this.includeKafkaMetadata = includeKafkaMetadata;
        this.errorReporterMethod = errorReporterMethod;
    }

    @Override
    public void processFailure(WriteBatch batch, Throwable throwable) {
        logger.error("Batch failed; size: {}; cause: {}", batch.getItems().length, throwable.getMessage());
        if (this.includeKafkaMetadata) {
            logger.error("Logging Kafka record metadata for each failed document");
        }
        for (WriteEvent writeEvent : batch.getItems()) {
            DocumentMetadataWriteHandle writeHandle = writeEvent.getMetadata();
            if (writeHandle instanceof DocumentMetadataHandle) {
                if (this.includeKafkaMetadata) {
                    logFailedWriteEvent(writeEvent, (DocumentMetadataHandle) writeHandle);
                }
                if (writeHandle instanceof SinkRecordMetadataHandle) {
                    reportError(((SinkRecordMetadataHandle) writeHandle).getSinkRecord(), throwable, writeEvent);
                }
            }
        }
    }

    private void logFailedWriteEvent(WriteEvent writeEvent, DocumentMetadataHandle metadata) {
        DocumentMetadataHandle.DocumentMetadataValues values = metadata.getMetadataValues();
        if (values != null) {
            logger.error("URI: {}; key: {}; partition: {}; offset: {}; timestamp: {}; topic: {}",
                writeEvent.getTargetUri(), values.get("kafka-key"), values.get("kafka-partition"),
                values.get("kafka-offset"), values.get("kafka-timestamp"), values.get("kafka-topic"));
        }
    }

    private void reportError(SinkRecord sinkRecord, Throwable throwable, WriteEvent writeEvent) {
        WriteBatcherSinkTask.addFailureHeaders(sinkRecord, throwable, MARKLOGIC_WRITE_FAILURE, writeEvent);
        Exception reportedException;
        if (throwable instanceof Exception) {
            reportedException = (Exception) throwable;
        } else {
            reportedException = new IOException(throwable.getMessage());
        }
        errorReporterMethod.accept(sinkRecord, reportedException);
    }
}
