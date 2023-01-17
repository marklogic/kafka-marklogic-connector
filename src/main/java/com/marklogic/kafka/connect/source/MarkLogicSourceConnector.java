package com.marklogic.kafka.connect.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

/**
 * Entry point into the MarkLogic source connector for Kafka.
 */
public class MarkLogicSourceConnector extends SourceConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    static final String MARKLOGIC_SOURCE_CONNECTOR_VERSION = MarkLogicSourceConnector.class.getPackage().getImplementationVersion();

    private Map<String, String> config;

    @Override
    public ConfigDef config() {
        return MarkLogicSourceConfig.CONFIG_DEF;
    }

    @Override
    public void start(final Map<String, String> config) {
        logger.info("Starting MarkLogicSourceConnector");
        Boolean configuredForDsl = StringUtils.hasText(config.get(MarkLogicSourceConfig.DSL_QUERY));
        Boolean configuredForSerialized = StringUtils.hasText(config.get(MarkLogicSourceConfig.SERIALIZED_QUERY));
        if ((!(configuredForDsl || configuredForSerialized)) || (configuredForDsl && configuredForSerialized)) {
            throw new ConfigException(
                format("Either a DSL Optic query (%s) or a serialized Optic query (%s), but not both, are required",
                    MarkLogicSourceConfig.DSL_QUERY, MarkLogicSourceConfig.SERIALIZED_QUERY)
            );
        }
        this.config = config;
    }

    @Override
    public void stop() {
        logger.info("Stopping MarkLogicSourceConnector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RowManagerSourceTask.class;
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
