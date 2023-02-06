/*
 * Copyright (c) 2023 MarkLogic Corporation
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
        if (taskCount > 1) {
            logger.warn("As of the 1.8.0 release, the Kafka tasks.max property is ignored and a single source connector " +
                            "task is created. This prevents duplicate records from being created via multiple instances of " +
                            "the task with the exact same config.");
        }
        final List<Map<String, String>> configs = new ArrayList<>(1);
        configs.add(config);
        return configs;
    }

    @Override
    public String version() {
        return MARKLOGIC_SOURCE_CONNECTOR_VERSION;
    }

}
