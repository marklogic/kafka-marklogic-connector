package com.marklogic.kafka.connect.source;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.RowBatchFailureListener;
import com.marklogic.client.datamovement.RowBatcher;
import com.marklogic.client.ext.helper.LoggingObject;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;

public abstract class AbstractRowBatcherBuilder<T> extends LoggingObject {
    protected final DataMovementManager dataMovementManager;
    protected final Map<String, Object> parsedConfig;
    protected final String topic;

    AbstractRowBatcherBuilder(DataMovementManager dataMovementManager, Map<String, Object> parsedConfig) {
        this.dataMovementManager = dataMovementManager;
        this.parsedConfig = parsedConfig;
        this.topic = (String) parsedConfig.get(MarkLogicSourceConfig.TOPIC);
    }

    public abstract RowBatcher<T> newRowBatcher(List<SourceRecord> newSourceRecords);

    void logBatchError(Exception ex, String record) {
        logger.error("Failed to create or add SourceRecord from result row: " + record + "\n cause: " + ex.getMessage());
    }

    void logBatchError(Exception ex) {
        logger.error("Failed to create or add SourceRecord from result row, cause: " + ex.getMessage());
    }

    /**
     * Configure the given RowBatcher based on DMSDK-related options in the parsedConfig.
     *
     * @param parsedConfig - The complete configuration object including any transform parameters.
     * @param rowBatcher - The RowBatcher object to be configured.
     */
    protected void configureRowBatcher(Map<String, Object> parsedConfig, RowBatcher<?> rowBatcher) {
        Integer batchSize = (Integer) parsedConfig.get(MarkLogicSourceConfig.DMSDK_BATCH_SIZE);
        if (batchSize != null) {
            logger.debug("DMSDK batch size: " + batchSize);
            rowBatcher.withBatchSize(batchSize);
        }

        Integer threadCount = (Integer) parsedConfig.get(MarkLogicSourceConfig.DMSDK_THREAD_COUNT);
        if (threadCount != null) {
            logger.debug("DMSDK thread count: " + threadCount);
            rowBatcher.withThreadCount(threadCount);
        }

        String jobName = (String) parsedConfig.get(MarkLogicSourceConfig.JOB_NAME);
        if (StringUtils.hasText(jobName)) {
            logger.debug("DMSDK Job Name: " + jobName);
            rowBatcher.withJobName(jobName);
        }

        Boolean consistentSnapshot = (Boolean) parsedConfig.get(MarkLogicSourceConfig.CONSISTENT_SNAPSHOT);
        logger.debug("DMSDK Consistent Snapshot: " + consistentSnapshot);
        if (consistentSnapshot) {
            rowBatcher.withConsistentSnapshot();
        }

        rowBatcher.onFailure(this::onFailureHandler);
    }

    private void onFailureHandler(RowBatchFailureListener.RowBatchFailureEvent batch, Throwable throwable) {
        String message = throwable.getMessage();
        String errorString = "Column not found: ";
        int locationOfErrorString = message.indexOf(errorString);
        if (locationOfErrorString > -1) {
            int lengthOfErrorString = errorString.length();
            int locationOfNextSpace = message.indexOf(" ", locationOfErrorString+lengthOfErrorString);
            String columnName = message.substring(locationOfErrorString+lengthOfErrorString, locationOfNextSpace);
            logger.error("batch "+batch.getJobBatchNumber()+" failed due to missing column: " + columnName);
        } else {
            logger.error("batch " + batch.getJobBatchNumber() + " failed with error: " + throwable.getMessage());
        }
    }

    public static AbstractRowBatcherBuilder<?> newRowBatcherBuilder(DataMovementManager dataMovementManager, Map<String, Object> parsedConfig) {
        MarkLogicSourceConfig.OUTPUT_TYPE outputType = MarkLogicSourceConfig.OUTPUT_TYPE.valueOf((String) parsedConfig.get(MarkLogicSourceConfig.OUTPUT_FORMAT));
        final AbstractRowBatcherBuilder<?> rowBatcherBuilder;
        switch (outputType) {
            case JSON :
                rowBatcherBuilder = new JsonRowBatcherBuilder(dataMovementManager, parsedConfig);
                break;
            case XML:
                rowBatcherBuilder = new XmlRowBatcherBuilder(dataMovementManager, parsedConfig);
                break;
            case CSV:
                rowBatcherBuilder = new CsvRowBatcherBuilder(dataMovementManager, parsedConfig);
                break;
            default:
                throw new IllegalArgumentException("Unexpected output type: " + outputType);
        }
        return rowBatcherBuilder;
    }
}
