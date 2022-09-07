package com.marklogic.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Entry point into the MarkLogic sink connector for Kafka. Main purpose is to determine if the user wishes to use
 * Bulk Data Services or DMSDK for writing data to MarkLogic.
 */
public class MarkLogicSinkConnector extends SinkConnector {

    static final String MARKLOGIC_SINK_CONNECTOR_VERSION = MarkLogicSinkConnector.class.getPackage().getImplementationVersion();

    private Map<String, String> config;

    @Override
    public ConfigDef config() {
        return MarkLogicSinkConfig.CONFIG_DEF;
    }

    @Override
    public void start(final Map<String, String> config) {
        this.config = config;
    }

    @Override
    public void stop() {
    }

    @Override
    public Class<? extends Task> taskClass() {
        Class clazz = StringUtils.hasText(this.config.get(MarkLogicSinkConfig.BULK_DS_ENDPOINT_URI)) ?
            BulkDataServicesSinkTask.class :
            WriteBatcherSinkTask.class;
        LoggerFactory.getLogger(getClass()).info("Task class: " + clazz);
        return clazz;
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
        return MARKLOGIC_SINK_CONNECTOR_VERSION;
    }

}
