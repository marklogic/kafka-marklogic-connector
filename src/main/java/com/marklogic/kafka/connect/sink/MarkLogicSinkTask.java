package com.marklogic.kafka.connect.sink;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.kafka.connect.DefaultDatabaseClientConfigBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.*;

/**
 * Performs the actual work associated with ingesting new documents into MarkLogic based on data received via the
 * "put" method.
 */
public class MarkLogicSinkTask extends SinkTask {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicSinkTask.class);

    private DatabaseClient databaseClient;
    private DataMovementManager dataMovementManager;
    private WriteBatcher writeBatcher;
    private SinkRecordConverter sinkRecordConverter;
    private boolean logKeys = false;
    private boolean logHeaders = false;

    @Override
    public void start(final Map<String, String> config) {
        logger.info("Starting");

        Map<String, Object> parsedConfig = MarkLogicSinkConfig.CONFIG_DEF.parse(config);

        logKeys = (Boolean) parsedConfig.get(MarkLogicSinkConfig.LOGGING_RECORD_KEY);
        logHeaders = (Boolean) parsedConfig.get(MarkLogicSinkConfig.LOGGING_RECORD_HEADERS);

        sinkRecordConverter = new DefaultSinkRecordConverter(parsedConfig);

        DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder().buildDatabaseClientConfig(parsedConfig);
        databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);

        dataMovementManager = databaseClient.newDataMovementManager();

        writeBatcher = dataMovementManager.newWriteBatcher();
        configureWriteBatcher(parsedConfig, writeBatcher);

        writeBatcher.onBatchFailure((writeBatch, throwable) -> {
            int batchSize = writeBatch.getItems().length;
            logger.error("#error failed to write {} records", batchSize);
            logger.error("#error batch failure:", throwable);
        });

        final String flowName = (String) parsedConfig.get(MarkLogicSinkConfig.DATAHUB_FLOW_NAME);
        if (flowName != null && flowName.trim().length() > 0) {
            writeBatcher.onBatchSuccess(buildSuccessListener(flowName, parsedConfig, databaseClientConfig));
        }

        dataMovementManager.startJob(writeBatcher);

        logger.info("Started");
    }

    /**
     * Configure the given WriteBatcher based on DMSDK-related options in the parsedConfig.
     *
     * @param parsedConfig
     * @param writeBatcher
     */
    protected void configureWriteBatcher(Map<String, Object> parsedConfig, WriteBatcher writeBatcher) {
        Integer batchSize = (Integer)parsedConfig.get(MarkLogicSinkConfig.DMSDK_BATCH_SIZE);
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

        String temporalCollection = (String)parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_TEMPORAL_COLLECTION);
        if (StringUtils.hasText(temporalCollection)) {
            logger.info("Will add documents to temporal collection: " + temporalCollection);
            writeBatcher.withTemporalCollection(temporalCollection);
        }
    }

    /**
     * This is all specific to Kafka, as it involves reading inputs from the Kafka config map and then using them to
     * construct the reusable RunFlowWriteBatchListener.
     *
     * @param flowName
     * @param parsedConfig
     * @param databaseClientConfig
     */
    protected RunFlowWriteBatchListener buildSuccessListener(String flowName, Map<String, Object> parsedConfig, DatabaseClientConfig databaseClientConfig) {
        String logMessage = String.format("After ingesting a batch, will run flow '%s'", flowName);
        final String flowSteps = (String) parsedConfig.get(MarkLogicSinkConfig.DATAHUB_FLOW_STEPS);
        List<String> steps = null;
        if (flowSteps != null && flowSteps.trim().length() > 0) {
            steps = Arrays.asList(flowSteps.split(","));
            logMessage += String.format(" with steps '%s' constrained to the URIs in that batch", steps.toString());
        }
        logger.info(logMessage);

        RunFlowWriteBatchListener listener = new RunFlowWriteBatchListener(flowName, steps, databaseClientConfig);
        if (parsedConfig.containsKey(MarkLogicSinkConfig.DATAHUB_FLOW_LOG_RESPONSE)) {
            listener.setLogResponse((Boolean) parsedConfig.get(MarkLogicSinkConfig.DATAHUB_FLOW_LOG_RESPONSE));
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

    @Override
    public void stop() {
        logger.info("Stopping");
        if (writeBatcher != null) {
            writeBatcher.flushAndWait();
            dataMovementManager.stopJob(writeBatcher);
        }
        if (databaseClient != null) {
            databaseClient.release();
        }
        logger.info("Stopped");
    }

    /**
     * This is doing all the work of writing to MarkLogic, which includes calling flushAsync on the WriteBatcher.
     * Alternatively, could move the flushAsync call to an overridden flush() method. Kafka defaults to flushing every
     * 60000ms - this can be configured via the offset.flush.interval.ms property.
     * <p>
     * Because this is calling flushAsync, the batch size won't come into play unless the incoming collection has a
     * size equal to or greater than the batch size.
     *
     * @param records - The records retrieved from Kafka
     */
    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        final List<String> headers = new ArrayList<>();

        records.forEach(record -> {

            if (record == null) {
                logger.warn("Skipping null record object.");
            } else {
                if (logKeys) {
                    logger.info("#record key {}", record.key());
                }
                if (logHeaders) {
                    headers.clear();
                    record.headers().forEach(header -> {
                        headers.add(String.format("%s:%s", header.key(), header.value().toString()));
                    });
                    logger.info("#record headers: {}", headers);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Processing record value {} in topic {}", record.value(), record.topic());
                }
                if (record.value() != null) {
                    try {
                        writeBatcher.add(sinkRecordConverter.convert(record));
                    } catch (IOException e) {
                        logger.warn("IOException in converting the Sink Record. Record value {} in topic {}", record.value(), record.topic());
                    }
                } else {
                    logger.warn("Skipping record with null value - possibly a 'tombstone' message.");
                }
            }
        });

        if (writeBatcher != null) {
            writeBatcher.flushAsync();
        } else {
            logger.warn("writeBatcher is null - ignore this is you are running unit tests, otherwise this is a problem.");
        }
    }

    public String version() {
        return MarkLogicSinkConnector.MARKLOGIC_SINK_CONNECTOR_VERSION;
    }

    /**
     * Exposed to facilitate testing.
     *
     * @return
     */
    protected WriteBatcher getWriteBatcher() {
        return writeBatcher;
    }
}
