package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.datamovement.*;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.kafka.connect.DefaultDatabaseClientConfigBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Uses MarkLogic's Data Movement SDK (DMSDK) to write data to MarkLogic.
 */
public class RowBatcherSourceTask extends SourceTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, Object> parsedConfig;
    private DatabaseClient databaseClient;
    private DataMovementManager dataMovementManager;
    private long pollDelayMs = 1000L;
    private AbstractRowBatcherBuilder<?> rowBatcherBuilder;
    private RowBatcher<?> rowBatcher = null;
    private ConstraintValueStore constraintValueStore = null;

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
        logger.info("Starting RowBatcherSourceTask");
        parsedConfig = MarkLogicSourceConfig.CONFIG_DEF.parse(config);
        DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder().buildDatabaseClientConfig(parsedConfig);
        databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);
        dataMovementManager = databaseClient.newDataMovementManager();
        pollDelayMs = (Long) parsedConfig.get(MarkLogicSourceConfig.WAIT_TIME);
        constraintValueStore = ConstraintValueStore.newConstraintValueStore(databaseClient, parsedConfig);

        rowBatcherBuilder = AbstractRowBatcherBuilder.newRowBatcherBuilder(dataMovementManager, parsedConfig);
        logger.info("Started RowBatcherSourceTask");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // Temporary logging while testing CP to ensure the correct output format is being used
        QueryHandler queryHandler = QueryHandler.newQueryHandler(databaseClient, parsedConfig);
        logger.info("Polling; RowBatcherBuilder: {}; QueryHandler: {}; sleep time: {}ms",
            rowBatcherBuilder.getClass().getSimpleName(), queryHandler.getClass().getSimpleName(), pollDelayMs);

        Thread.sleep(pollDelayMs);

        List<SourceRecord> newSourceRecords = new Vector<>();
        long start = System.currentTimeMillis();
        try {
            getNewRowBatcher(newSourceRecords);
            String previousMaxConstraintColumnValue = null;
            if (constraintValueStore != null) {
                previousMaxConstraintColumnValue = constraintValueStore.retrievePreviousMaxConstraintColumnValue();
            }
            queryHandler.addQueryToRowBatcher(rowBatcher, previousMaxConstraintColumnValue);
            performPoll(queryHandler, newSourceRecords);
        } catch (Exception ex) {
            if (!rowBatcherErrorIsKnownServerBug(ex)) {
                logger.error("Unable to poll for source records; cause: " + ex.getMessage());
            }
            return null;
        }

        logger.info("Source record count: " + newSourceRecords.size() + "; duration: " + (System.currentTimeMillis() - start));
        return newSourceRecords.isEmpty() ? null : newSourceRecords;
    }

    protected void performPoll(QueryHandler queryHandler, List<SourceRecord> newSourceRecords) {
        dataMovementManager.startJob(rowBatcher);
        rowBatcher.awaitCompletion();
        dataMovementManager.stopJob(rowBatcher);

        if (constraintValueStore != null && !newSourceRecords.isEmpty()) {
            long queryStartTimeInMillis = rowBatcher.getServerTimestamp();
            String newMaxConstraintColumnValue = queryHandler.getMaxConstraintColumnValue(queryStartTimeInMillis);
            logger.info("Storing new max constraint value: " + newMaxConstraintColumnValue);
            constraintValueStore.storeConstraintState(newMaxConstraintColumnValue, newSourceRecords.size());
        }
        rowBatcher = null;
    }

    // Based on https://docs.confluent.io/platform/current/connect/devguide.html#task-example-source-task
    // This method needs to be synchronized "because SourceTasks are given a dedicated thread which they can block
    // indefinitely, so they need to be stopped with a call from a different thread in the Worker."
    @Override
    public synchronized void stop() {
        logger.info("Stop called; stopping job");
        if (rowBatcher != null) {
            dataMovementManager.stopJob(rowBatcher);
        }
        if (databaseClient != null) {
            databaseClient.release();
        }
    }

    private boolean rowBatcherErrorIsKnownServerBug(Exception ex) {
        if (ex instanceof FailedRequestException) {
            String serverMessage = ((FailedRequestException) ex).getServerMessage();
            final String knownBugErrorMessage = "$tableId as xs:string -- Invalid coercion: () as xs:string";
            if (serverMessage != null && serverMessage.contains(knownBugErrorMessage)) {
                logger.debug("Catching known bug where an error is thrown when no rows exist; will return no data instead");
                return true;
            }
        }
        return false;
    }

    protected RowBatcher<?> getNewRowBatcher(List<SourceRecord> newSourceRecords) {
        rowBatcher = rowBatcherBuilder.newRowBatcher(newSourceRecords);
        return rowBatcher;
    }

    protected String getPreviousMaxConstraintColumnValue() {
        if (constraintValueStore != null) {
            return constraintValueStore.retrievePreviousMaxConstraintColumnValue();
        }
        return null;
    }
}
