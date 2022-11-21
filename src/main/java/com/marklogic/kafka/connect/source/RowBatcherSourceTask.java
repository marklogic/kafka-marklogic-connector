package com.marklogic.kafka.connect.source;

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

    /**
     * Required for a Kafka task.
     *
     * @return
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
        Map<String, Object> parsedConfig = MarkLogicSourceConfig.CONFIG_DEF.parse(config);
        logger.info("Started");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        logger.info("poll not yet implemented");

        Thread.sleep(1000);
        return null;
    }

    @Override
    public void stop() {
        logger.info("Stopping RowBatcherSourceTask");
    }
}
