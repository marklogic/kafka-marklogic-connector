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
package com.marklogic.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Entry point into the MarkLogic sink connector for Kafka. Main purpose is to
 * determine if the user wishes to use
 * Bulk Data Services or DMSDK for writing data to MarkLogic.
 */
public class MarkLogicSinkConnector extends SinkConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    static final String MARKLOGIC_SINK_CONNECTOR_VERSION = MarkLogicSinkConnector.class.getPackage()
            .getImplementationVersion();

    private Map<String, String> config;

    @Override
    public ConfigDef config() {
        return MarkLogicSinkConfig.CONFIG_DEF;
    }

    @Override
    public void start(final Map<String, String> config) {
        logger.info("Starting MarkLogicSinkConnector");
        this.config = config;
    }

    @Override
    public void stop() {
        logger.info("Stopping MarkLogicSinkConnector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        Class<? extends Task> clazz = StringUtils.hasText(this.config.get(MarkLogicSinkConfig.BULK_DS_ENDPOINT_URI))
                ? BulkDataServicesSinkTask.class
                : WriteBatcherSinkTask.class;
        LoggerFactory.getLogger(getClass()).info("Task class: {}", clazz);
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
