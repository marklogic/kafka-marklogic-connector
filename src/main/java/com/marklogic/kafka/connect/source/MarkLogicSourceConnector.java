package com.marklogic.kafka.connect.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Entry point into the MarkLogic source connector for Kafka.
 */
public class MarkLogicSourceConnector extends SourceConnector {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    static final String MARKLOGIC_SOURCE_CONNECTOR_VERSION = MarkLogicSourceConnector.class.getPackage().getImplementationVersion();

    private Map<String, String> config;

    @Override
    public ConfigDef config() {
        return MarkLogicSourceConfig.CONFIG_DEF;
    }

    @Override
    public void start(final Map<String, String> config) {
        logger.info("Starting MarkLogicSourceConnector");
        this.config = config;
    }

    @Override
    public void stop() {
        logger.info("Stopping MarkLogicSourceConnector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RowBatcherSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int taskCount) {
        final List<Map<String, String>> configs = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; ++i) {
            configs.add(config);
        }
        return configs;
    }

    @Override
    public String version() {
        return MARKLOGIC_SOURCE_CONNECTOR_VERSION;
    }

}
