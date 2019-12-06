package com.marklogic.kafka.connect.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MarkLogicSourceConnector extends SourceConnector {

    static final String MARKLOGIC_SOURCE_CONNECTOR_VERSION = "0.1.0";

    private Map<String, String> config;

    @Override
    public void start(Map<String, String> props) {
        config = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MarkLogicClientSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() { }

    @Override
    public ConfigDef config() {
        return MarkLogicSourceConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return MARKLOGIC_SOURCE_CONNECTOR_VERSION;
    }
}
