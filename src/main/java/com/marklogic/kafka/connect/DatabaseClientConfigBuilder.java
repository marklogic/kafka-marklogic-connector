/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect;

import com.marklogic.client.ext.DatabaseClientConfig;

import java.util.Map;

/**
 * Defines how a map of properties read in by Kafka are used to build an instance of DatabaseClientConfig.
 */
public interface DatabaseClientConfigBuilder {

    DatabaseClientConfig buildDatabaseClientConfig(Map<String, Object> kafkaConfig);

}
