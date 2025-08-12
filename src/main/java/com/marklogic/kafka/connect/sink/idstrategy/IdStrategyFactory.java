/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink.idstrategy;

import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;

import java.util.Map;

public interface IdStrategyFactory {

    public static IdStrategy getIdStrategy(Map<String, Object> parsedConfig) {
        String strategyType = (String) parsedConfig.get(MarkLogicSinkConfig.ID_STRATEGY);
        String strategyPaths = (String) parsedConfig.get(MarkLogicSinkConfig.ID_STRATEGY_PATH);

        switch ((strategyType != null) ? strategyType : "UUID") {
            case "JSONPATH":
                return (new JSONPathStrategy(strategyPaths.trim().split(",")[0]));
            case "HASH":
                return (new HashedJSONPathsStrategy(strategyPaths.trim().split(",")));
            case "KAFKA_META_WITH_SLASH":
                return (new KafkaMetaStrategy());
            case "KAFKA_META_HASHED":
                return (new HashedKafkaMetaStrategy());
            case "UUID":
            default:
                return (new DefaultStrategy());
        }
    }

}
