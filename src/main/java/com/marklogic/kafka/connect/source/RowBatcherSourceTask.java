package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.datamovement.*;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
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
    private long pollDelayMs = 1000L;
    private QueryContextBuilder<?> queryContextBuilder;
    private QueryContext<?> queryContext;
    private String previousMaxConstraintColumnValue = null;
    private String constraintColumn = null;

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
        Map<String, Object> parsedConfig = MarkLogicSourceConfig.CONFIG_DEF.parse(config);
        DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder().buildDatabaseClientConfig(parsedConfig);
        this.databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);
        dataMovementManager = databaseClient.newDataMovementManager();
        pollDelayMs = (Long) parsedConfig.get(MarkLogicSourceConfig.WAIT_TIME);
        this.constraintColumn = (String) parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME);
        if (!StringUtils.hasText(constraintColumn)) {
            constraintColumn = null;
        }

        queryContextBuilder = AbstractRowBatchBuilder.newQueryContextBuilder(dataMovementManager, parsedConfig);
        logger.info("Started RowBatcherSourceTask");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // Temporary logging while testing CP to ensure the correct output format is being used
        logger.info("Polling; QueryContextBuilder: " + queryContextBuilder.getClass().getName());

        List<SourceRecord> newSourceRecords = new Vector<>();
        logger.info("Temporary log statement for testing; sleeping for " + pollDelayMs + "ms");
        Thread.sleep(pollDelayMs);

        long start = System.currentTimeMillis();
        try {
            loadNewQueryContext(newSourceRecords);
       } catch (Exception ex) {
            if (!rowBatcherErrorIsKnownServerBug(ex)) {
                logger.error("Unable to poll for source records. Unable to initialize row batcher; cause: " + ex.getMessage());
            }
            return null;
        }

        performPoll();
        logger.info("Source record count: " + newSourceRecords.size() + "; duration: " + (System.currentTimeMillis() - start));
        return newSourceRecords.isEmpty() ? null : newSourceRecords;
    }

    protected void loadNewQueryContext(List<SourceRecord> newSourceRecords) {
        queryContext = queryContextBuilder.newQueryContext(newSourceRecords, previousMaxConstraintColumnValue);
    }

    protected void performPoll() {
        try {
            logger.info("Starting job");
            RowBatcher<?> rowBatcher = queryContext.getRowBatcher();
            dataMovementManager.startJob(rowBatcher);
            logger.info("Awaiting completion");
            rowBatcher.awaitCompletion();
            dataMovementManager.stopJob(rowBatcher);

            if (constraintColumn != null) {
                long queryStartTimeInMillis = rowBatcher.getServerTimestamp();
                previousMaxConstraintColumnValue = getBatchMaxValue(queryContext.getCurrentQuery(), queryStartTimeInMillis, constraintColumn);
            }
        } catch (Exception ex) {
            logger.error("Unable to poll for source records. Job failed to complete successfully; cause: " + ex.getMessage());
        } finally {
            queryContext.clearRowBatcher();
        }
    }

    // Based on https://docs.confluent.io/platform/current/connect/devguide.html#task-example-source-task
    // This method needs to be synchronized "because SourceTasks are given a dedicated thread which they can block
    // indefinitely, so they need to be stopped with a call from a different thread in the Worker."
    @Override
    public synchronized void stop() {
        logger.info("Stop called; stopping job");
        RowBatcher<?> rowBatcher = queryContext.getRowBatcher();
        if (rowBatcher != null) {
            dataMovementManager.stopJob(rowBatcher);
        }
        if (databaseClient != null) {
            databaseClient.release();
        }
    }

    private boolean rowBatcherErrorIsKnownServerBug(Exception ex) {
        if (ex instanceof FailedRequestException) {
            String serverMessage = ((FailedRequestException)ex).getServerMessage();
            final String knownBugErrorMessage = "$tableId as xs:string -- Invalid coercion: () as xs:string";
            if (serverMessage != null && serverMessage.contains(knownBugErrorMessage)) {
                logger.debug("Catching known bug where an error is thrown when no rows exist; will return no data instead");
                return true;
            }
        }
        return false;
    }

    private String buildMaxValueDslQuery(String currentQuery, String constraintColumn) {
        return String.format("%s.orderBy(op.desc(\"%s\")).limit(1).select([op.as(\"constraint\", op.col(\"%s\"))])", currentQuery, constraintColumn, constraintColumn);
    }

    private String getBatchMaxValue(String currentQuery, long queryStartTimeInMillis, String constraintColumn) {
        String currentMaxValueQuery = buildMaxValueDslQuery(currentQuery, constraintColumn);

        RowManager rowMgr = databaseClient.newRowManager();
        RawQueryDSLPlan plan = rowMgr.newRawQueryDSLPlan(new StringHandle(currentMaxValueQuery));
        JacksonHandle handle = new JacksonHandle().withFormat(Format.JSON).withMimetype("application/json");
        handle.setPointInTimeQueryTimestamp(queryStartTimeInMillis);
        JacksonHandle result = rowMgr.resultDoc(plan, handle);
        return result.get().get("rows").get(0).get("constraint").get("value").toString();
    }

    protected String getPreviousMaxConstraintColumnValue() {
        return previousMaxConstraintColumnValue;
    }

    protected QueryContext<?> getQueryContext() {
        return queryContext;
    }
}
