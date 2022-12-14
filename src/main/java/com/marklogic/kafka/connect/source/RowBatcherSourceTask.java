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

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private DatabaseClient databaseClient;
    private DataMovementManager dataMovementManager;
    private RowBatcher<?> rowBatcher = null;
    private long pollDelayMs = 1000L;
    private RowBatcherBuilder<?> rowBatcherBuilder;

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
        Map<String, Object> parsedConfig = MarkLogicSourceConfig.CONFIG_DEF.parse(config);
        DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder().buildDatabaseClientConfig(parsedConfig);
        this.databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);
        dataMovementManager = databaseClient.newDataMovementManager();
        pollDelayMs = (Long) parsedConfig.get(MarkLogicSourceConfig.WAIT_TIME);

        rowBatcherBuilder = newRowBatcherBuilder(dataMovementManager, parsedConfig);
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

    protected RowBatcher<?> newRowBatcher(List<SourceRecord> newSourceRecords) {
        return rowBatcherBuilder.newRowBatcher(newSourceRecords);
    }

    private RowBatcherBuilder<?> newRowBatcherBuilder(DataMovementManager dataMovementManager, Map<String, Object> parsedConfig) {
        MarkLogicSourceConfig.OUTPUT_TYPE outputType = MarkLogicSourceConfig.OUTPUT_TYPE.valueOf((String) parsedConfig.get(MarkLogicSourceConfig.OUTPUT_FORMAT));
        final RowBatcherBuilder<?> rowBatcherBuilder;
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
