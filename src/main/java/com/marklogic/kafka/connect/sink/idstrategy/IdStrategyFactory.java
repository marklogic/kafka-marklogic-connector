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
