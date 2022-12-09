package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.datamovement.*;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.ContentHandle;
import com.marklogic.client.row.RawQueryDSLPlan;
import com.marklogic.client.row.RowManager;
import com.marklogic.kafka.connect.DefaultDatabaseClientConfigBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;

/**
 * Uses MarkLogic's Data Movement SDK (DMSDK) to write data to MarkLogic.
 */
public class RowBatcherSourceTask extends SourceTask {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private DatabaseClient databaseClient;
    private DataMovementManager dataMovementManager;
    private RowBatcher<JsonNode> rowBatcher = null;
    private Map<String, Object> parsedConfig;
    private long pollDelayMs = 1000L;

    /**
     * Required for a Kafka task.
     *
     * @return - Returns the version of the MarkLogic source connector
     */
    @Override
    public String version() {
        return MarkLogicSourceConnector.MARKLOGIC_SOURCE_CONNECTOR_VERSION;
    }

    /**
     * Invoked by Kafka when the connector is started by Kafka Connect.
     *
     * @param config initial configuration
     */
    @Override
    public final void start(Map<String, String> config) {
        logger.debug("Starting RowBatcherSourceTask");
        this.parsedConfig = MarkLogicSourceConfig.CONFIG_DEF.parse(config);
        DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder().buildDatabaseClientConfig(parsedConfig);
        this.databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);
        dataMovementManager = databaseClient.newDataMovementManager();
        pollDelayMs = (Long) parsedConfig.get(MarkLogicSourceConfig.WAIT_TIME);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> newSourceRecords = new Vector<>();
        logger.info("Temporary log statement for testing; sleeping for " + pollDelayMs + "ms");
        Thread.sleep(pollDelayMs);

        try {
            rowBatcher = newRowBatcher(newSourceRecords);
        } catch (FailedRequestException ex) {
            if (!rowBatcherErrorIsKnownServerBug(ex)) {
                logger.error("Unable to poll for source records. Unable to initialize row batcher; cause: " + ex.getMessage());
            }
            return null;
        } catch (Exception ex) {
            logger.error("Unable to poll for source records. Unable to initialize row batcher; cause: " + ex.getMessage());
            return null;
        }

        performPoll();
        return newSourceRecords.isEmpty() ? null : newSourceRecords;
    }

    protected RowBatcher<JsonNode> newRowBatcher(List<SourceRecord> newSourceRecords) {
        ContentHandle<JsonNode> jsonHandle = new JacksonHandle().withFormat(Format.JSON).withMimetype("application/json");
        rowBatcher =  dataMovementManager.newRowBatcher(jsonHandle);
        configureRowBatcher(parsedConfig, rowBatcher, newSourceRecords);
        return rowBatcher;
    }

    protected void performPoll() {
        try {
            dataMovementManager.startJob(rowBatcher);
            rowBatcher.awaitCompletion();
            dataMovementManager.stopJob(rowBatcher);
        } catch (Exception ex) {
            logger.error("Unable to poll for source records. Job failed to complete successfully; cause: " + ex.getMessage());
        } finally {
            rowBatcher = null;
        }
    }

    // Based on https://docs.confluent.io/platform/current/connect/devguide.html#task-example-source-task
    // This method needs to be synchronized "because SourceTasks are given a dedicated thread which they can block
    // indefinitely, so they need to be stopped with a call from a different thread in the Worker."
    @Override
    public synchronized void stop() {
        if (rowBatcher != null) {
            dataMovementManager.stopJob(rowBatcher);
        }
        if (databaseClient != null) {
            databaseClient.release();
        }
    }

    /**
     * Configure the given RowBatcher based on DMSDK-related options in the parsedConfig.
     *
     * @param parsedConfig - The complete configuration object including any transform parameters.
     * @param rowBatcher - The RowBatcher object to be configured.
     */
    private void configureRowBatcher(Map<String, Object> parsedConfig, RowBatcher<JsonNode> rowBatcher, List<SourceRecord> newSourceRecords) {
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


        String dslPlan = (String) parsedConfig.get(MarkLogicSourceConfig.DSL_PLAN);
        RowManager rowMgr = rowBatcher.getRowManager();
        RawQueryDSLPlan plan = rowMgr.newRawQueryDSLPlan(new StringHandle(dslPlan));
        rowBatcher.withBatchView(plan);
        rowBatcher.onSuccess(event -> onSuccessHandler(event, newSourceRecords));
        rowBatcher.onFailure(this::onFailureHandler);
    }

    private void onSuccessHandler(RowBatchSuccessListener.RowBatchResponseEvent<JsonNode> event, List<SourceRecord> newSourceRecords) {
        JsonNode rows = event.getRowsDoc().get("rows");
        logger.debug("JsonNode: \n" + rows.toPrettyString());

        String topic = (String) parsedConfig.get(MarkLogicSourceConfig.TOPIC);
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
                logger.error("Failed to create or add SourceRecord from result row; cause: " + ex.getMessage());
                logger.error("Failed result row: " + row.toString());
            }
        }
    }

    private void onFailureHandler(RowBatchFailureListener.RowBatchFailureEvent batch, Throwable throwable) {
        logger.error("batch "+batch.getJobBatchNumber()+" failed with error: "+throwable.getMessage());
    }

    private boolean rowBatcherErrorIsKnownServerBug(FailedRequestException ex) {
        String serverMessage = ex.getServerMessage();
        final String knownBugErrorMessage = "$tableId as xs:string -- Invalid coercion: () as xs:string";
        if (serverMessage != null && serverMessage.contains(knownBugErrorMessage)) {
            logger.debug("Catching known bug where an error is thrown when no rows exist; will return no data instead");
            return true;
        }
        return false;
    }
}
