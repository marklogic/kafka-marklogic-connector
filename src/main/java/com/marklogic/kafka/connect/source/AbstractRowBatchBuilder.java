package com.marklogic.kafka.connect.source;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.RowBatchFailureListener;
import com.marklogic.client.datamovement.RowBatcher;
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RawPlanDefinition;
import com.marklogic.client.row.RawQueryDSLPlan;
import com.marklogic.client.row.RowManager;
import org.springframework.util.StringUtils;

import java.util.Map;

public class AbstractRowBatchBuilder extends LoggingObject {
    protected final DataMovementManager dataMovementManager;
    protected final Map<String, Object> parsedConfig;
    protected final String topic;
    protected final String constraintColumn;
    protected String currentQuery;

    AbstractRowBatchBuilder(DataMovementManager dataMovementManager, Map<String, Object> parsedConfig) {
        this.dataMovementManager = dataMovementManager;
        this.parsedConfig = parsedConfig;
        this.topic = (String) parsedConfig.get(MarkLogicSourceConfig.TOPIC);
        this.constraintColumn = (String) parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME);
    }

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
    protected void configureRowBatcher(Map<String, Object> parsedConfig, RowBatcher<?> rowBatcher, String previousMaxConstraintColumnValue) {
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

        configureBatchView(parsedConfig, rowBatcher, previousMaxConstraintColumnValue);
        rowBatcher.onFailure(this::onFailureHandler);
    }

    private void configureBatchView(Map<String, Object> parsedConfig, RowBatcher<?> rowBatcher, String previousMaxConstraintColumnValue) {
        boolean configuredForDsl = StringUtils.hasText((String) parsedConfig.get(MarkLogicSourceConfig.DSL_QUERY));
        if (configuredForDsl) {
            currentQuery = (String) parsedConfig.get(MarkLogicSourceConfig.DSL_QUERY);
            if (previousMaxConstraintColumnValue != null) {
                currentQuery = injectConstraintIntoDslQuery(currentQuery, constraintColumn, previousMaxConstraintColumnValue);
            }
            logger.info("constrainedQuery (unless initial run): " + currentQuery);
            RowManager rowMgr = rowBatcher.getRowManager();
            RawQueryDSLPlan query = rowMgr.newRawQueryDSLPlan(new StringHandle(currentQuery));
            rowBatcher.withBatchView(query);
        } else {
            String serializedQuery = (String) parsedConfig.get(MarkLogicSourceConfig.SERIALIZED_QUERY);
            RowManager rowMgr = rowBatcher.getRowManager();
            RawPlanDefinition query = rowMgr.newRawPlanDefinition(new StringHandle(serializedQuery));
            rowBatcher.withBatchView(query);
        }
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

    protected static String injectConstraintIntoDslQuery(String originalDsl, String constraintColumn, String constraintValue) {
        String constraintPhrase = ".where(op.gt(op.col(\"" + constraintColumn + "\"), " + constraintValue + "))";
        String constrainedDsl;
        String regexToRemoveNonQuotedWhitespace = "\\s+(?=(?:[^\\'\"]*[\\'\"][^\\'\"]*[\\'\"])*[^\\'\"]*$)";
        String originalDslNoWhitespace = originalDsl.replaceAll(regexToRemoveNonQuotedWhitespace, "");
        if (originalDslNoWhitespace.contains(").")) {
            int firstClosingParen = originalDslNoWhitespace.indexOf(").");
            constrainedDsl = originalDslNoWhitespace.substring(0, firstClosingParen+1)
                + constraintPhrase + originalDslNoWhitespace.substring(firstClosingParen+1);
        } else {
            constrainedDsl = originalDsl + constraintPhrase;
        }
        return constrainedDsl;
    }

    public static QueryContextBuilder<?> newQueryContextBuilder(DataMovementManager dataMovementManager, Map<String, Object> parsedConfig) {
        MarkLogicSourceConfig.OUTPUT_TYPE outputType = MarkLogicSourceConfig.OUTPUT_TYPE.valueOf((String) parsedConfig.get(MarkLogicSourceConfig.OUTPUT_FORMAT));
        final QueryContextBuilder<?> queryContextBuilder;
        switch (outputType) {
            case JSON :
                queryContextBuilder = new JsonQueryContextBuilder(dataMovementManager, parsedConfig);
                break;
            case XML:
                queryContextBuilder = new XmlQueryContextBuilder(dataMovementManager, parsedConfig);
                break;
            case CSV:
                queryContextBuilder = new CsvQueryContextBuilder(dataMovementManager, parsedConfig);
                break;
            default:
                throw new IllegalArgumentException("Unexpected output type: " + outputType);
        }
        return queryContextBuilder;
    }
}
