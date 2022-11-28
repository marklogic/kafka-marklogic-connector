package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
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

import java.util.List;
import java.util.Map;

/**
 * Uses MarkLogic's Data Movement SDK (DMSDK) to write data to MarkLogic.
 */
public class RowBatcherSourceTask extends SourceTask {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private DatabaseClient databaseClient;
    private DataMovementManager dataMovementManager;
    private RowBatcher<JsonNode> rowBatcher = null;
    private Map<String, Object> parsedConfig;

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
        logger.info("Starting");
        logger.info(String.valueOf(config));
        this.parsedConfig = MarkLogicSourceConfig.CONFIG_DEF.parse(config);
        DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder().buildDatabaseClientConfig(parsedConfig);
        this.databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);
        dataMovementManager = databaseClient.newDataMovementManager();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        logger.info("poll not yet implemented");

        rowBatcher =  getNewRowBatcher();
        performPoll();

        Thread.sleep(1000);
        return null;
    }

    protected RowBatcher<JsonNode> getNewRowBatcher() {
        ContentHandle<JsonNode> jsonHandle = new JacksonHandle().withFormat(Format.JSON).withMimetype("application/json");
        rowBatcher =  dataMovementManager.newRowBatcher(jsonHandle);
        configureRowBatcher(parsedConfig, rowBatcher);
        return rowBatcher;
    }

    protected void performPoll() {
        dataMovementManager.startJob(rowBatcher);
        rowBatcher.awaitCompletion();
        dataMovementManager.stopJob(rowBatcher);
        rowBatcher = null;
    }

    @Override
    public void stop() {
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
    private void configureRowBatcher(Map<String, Object> parsedConfig, RowBatcher<JsonNode> rowBatcher) {
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


        String dslPlan = (String) parsedConfig.get(MarkLogicSourceConfig.DSL_PLAN);
        RowManager rowMgr = rowBatcher.getRowManager();
        RawQueryDSLPlan plan = rowMgr.newRawQueryDSLPlan(new StringHandle(dslPlan));
        rowBatcher.withBatchView(plan);

        rowBatcher.onSuccess(event -> {
            JsonNode rows = event.getRowsDoc().get("rows");
            logger.info("JsonNode: \n" + rows.toPrettyString());
        });
        rowBatcher.onFailure((batch, throwable) -> logger.warn("batch "+batch.getJobBatchNumber()+" failed with error: "+throwable.getMessage()));
    }
}
