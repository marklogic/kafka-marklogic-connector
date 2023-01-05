package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.kafka.connect.DefaultDatabaseClientConfigBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Uses MarkLogic's Data Movement SDK (DMSDK) to write data to MarkLogic.
 */
public class RowBatcherSourceTask extends SourceTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<String, Object> parsedConfig;
    private DatabaseClient databaseClient;
    private long pollDelayMs = 1000L;
    private ConstraintValueStore constraintValueStore = null;
    private String topic;

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
        pollDelayMs = (Long) parsedConfig.get(MarkLogicSourceConfig.WAIT_TIME);
        constraintValueStore = ConstraintValueStore.newConstraintValueStore(databaseClient, parsedConfig);
        this.topic = (String) parsedConfig.get(MarkLogicSourceConfig.TOPIC);
        logger.info("Started RowBatcherSourceTask");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        logger.info("Polling; sleep time: {}ms", pollDelayMs);
        Thread.sleep(pollDelayMs);

        try {
            final String previousMaxConstraintColumnValue = constraintValueStore != null ?
                constraintValueStore.retrievePreviousMaxConstraintColumnValue() : null;

            QueryHandler queryHandler = QueryHandler.newQueryHandler(databaseClient, parsedConfig);
            PlanBuilder.Plan plan = queryHandler.newPlan(previousMaxConstraintColumnValue);
            final long start = System.currentTimeMillis();
            PlanInvoker.Results results = PlanInvoker.newPlanInvoker(databaseClient, parsedConfig).invokePlan(plan, topic);
            final long duration = System.currentTimeMillis() - start;
            List<SourceRecord> newSourceRecords = results.getSourceRecords();
            logger.info("Source record count: " + newSourceRecords.size() + "; duration: " + duration);
            updateMaxConstraintValue(results, queryHandler);
            return newSourceRecords.isEmpty() ? null : newSourceRecords;
        } catch (Exception ex) {
            // TODO Will include query here soon
            logger.error("Unable to poll for source records; cause: " + ex.getMessage(), ex);
            return null;
        }
    }

    // Based on https://docs.confluent.io/platform/current/connect/devguide.html#task-example-source-task
    // This method needs to be synchronized "because SourceTasks are given a dedicated thread which they can block
    // indefinitely, so they need to be stopped with a call from a different thread in the Worker."
    @Override
    public synchronized void stop() {
        logger.info("Stop called; releasing DatabaseClient");
        if (databaseClient != null) {
            databaseClient.release();
        }
    }

    private void updateMaxConstraintValue(PlanInvoker.Results results, QueryHandler queryHandler) {
        if (constraintValueStore != null && !results.getSourceRecords().isEmpty()) {
            long serverTimestamp = results.getServerTimestamp();
            String newMaxConstraintColumnValue = queryHandler.getMaxConstraintColumnValue(serverTimestamp);
            logger.info("Storing new max constraint value: " + newMaxConstraintColumnValue);
            constraintValueStore.storeConstraintState(newMaxConstraintColumnValue, results.getSourceRecords().size());
        }
    }

    protected String getPreviousMaxConstraintColumnValue() {
        if (constraintValueStore != null) {
            return constraintValueStore.retrievePreviousMaxConstraintColumnValue();
        }
        return null;
    }
}
