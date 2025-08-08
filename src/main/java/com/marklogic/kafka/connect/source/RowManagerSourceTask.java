/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * Uses a RowManager to read rows from MarkLogic in a single call. RowBatcher is
 * not used as it
 * introduces a significant amount of complexity, along with a limitation where
 * users can only use the
 * fromView accessor. And the performance benefits associated with RowBatcher
 * and its support for parallel reads
 * and batches are realized more when a user is reading large numbers of rows -
 * like hundreds of thousands
 * or more - but at that point, the user runs the risk of running out of memory
 * in Kafka Connect due to a
 * very large list. The thought then is that users are far more likely to want
 * to read smaller numbers of
 * rows at one time and use a constraint column to throttle how much is returned
 * at once.
 */
public class RowManagerSourceTask extends SourceTask {

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
        logger.info("Starting RowManagerSourceTask");
        parsedConfig = MarkLogicSourceConfig.CONFIG_DEF.parse(config);
        DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder()
                .buildDatabaseClientConfig(parsedConfig);
        databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);
        pollDelayMs = (Long) parsedConfig.get(MarkLogicSourceConfig.WAIT_TIME);
        constraintValueStore = ConstraintValueStore.newConstraintValueStore(databaseClient, parsedConfig);
        this.topic = (String) parsedConfig.get(MarkLogicSourceConfig.TOPIC);
        logger.info("Started RowManagerSourceTask");
    }

    @SuppressWarnings("java:S1168") // Kafka prefers for null to be returned when no data exists
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        logger.debug("Polling; sleep time: {}ms", pollDelayMs);
        Thread.sleep(pollDelayMs);

        String currentQuery = "<Not Built Yet>";
        try {
            final String previousMaxConstraintColumnValue = constraintValueStore != null
                    ? constraintValueStore.retrievePreviousMaxConstraintColumnValue()
                    : null;

            QueryHandler queryHandler = QueryHandler.newQueryHandler(databaseClient, parsedConfig);
            PlanBuilder.Plan plan = queryHandler.newPlan(previousMaxConstraintColumnValue);
            currentQuery = queryHandler.getCurrentQuery();
            final long start = System.currentTimeMillis();
            PlanInvoker.Results results = PlanInvoker.newPlanInvoker(databaseClient, parsedConfig).invokePlan(plan,
                    topic);
            final long duration = System.currentTimeMillis() - start;
            List<SourceRecord> newSourceRecords = results.getSourceRecords();
            if (!newSourceRecords.isEmpty()) {
                logger.info("Source record count: {}; duration: {}", newSourceRecords.size(), duration);
            } else {
                logger.debug("No source records found; duration: {}", duration);
            }
            updateMaxConstraintValue(results, queryHandler);
            return newSourceRecords.isEmpty() ? null : newSourceRecords;
        } catch (Exception ex) {
            final String message = String.format("Unable to poll for source records; cause: %s; query: %s",
                    ex.getMessage(), currentQuery);
            if (logger.isDebugEnabled()) {
                logger.error(message, ex);
            } else {
                logger.error(message);
            }
            return null;
        }
    }

    // Based on
    // https://docs.confluent.io/platform/current/connect/devguide.html#task-example-source-task
    // This method needs to be synchronized "because SourceTasks are given a
    // dedicated thread which they can block
    // indefinitely, so they need to be stopped with a call from a different thread
    // in the Worker."
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
            logger.debug("Storing new max constraint value: {}", newMaxConstraintColumnValue);
            constraintValueStore.storeConstraintState(newMaxConstraintColumnValue, results.getSourceRecords().size());
        }
    }

    protected String getPreviousMaxConstraintColumnValue() {
        if (constraintValueStore != null) {
            return constraintValueStore.retrievePreviousMaxConstraintColumnValue();
        }
        return null;
    }

    /**
     * Exists only for testing.
     *
     * @param constraintValueStore
     */
    protected void setConstraintValueStore(ConstraintValueStore constraintValueStore) {
        this.constraintValueStore = constraintValueStore;
    }
}
