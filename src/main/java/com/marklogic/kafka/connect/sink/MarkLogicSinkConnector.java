package com.marklogic.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MarkLogicSinkConnector extends SinkConnector {

    static final String MARKLOGIC_SINK_CONNECTOR_VERSION = MarkLogicSinkConnector.class.getPackage().getImplementationVersion();

    private Map<String, String> config;

    @Override
    public ConfigDef config() {
        return MarkLogicSinkConfig.CONFIG_DEF;
    }

    @Override
    public void start(final Map<String, String> arg0) {
        config = arg0;
    }

    @Override
    public void stop() {
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MarkLogicSinkTask.class;
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
