package com.marklogic.client.id.strategy;

import java.util.Map;

import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;

public  class IdStrategyFactory {
	
	public static IdStrategy getIdStrategy(Map<String, String> kafkaConfig) {
		String strategyType = kafkaConfig.get(MarkLogicSinkConfig.ID_STRATEGY);
		String strategyPaths= kafkaConfig.get(MarkLogicSinkConfig.ID_STRATEGY_PATH);
		
		switch((strategyType != null) ? strategyType : "UUID") {
			case "JSONPATH":
				return (new JSONPathStrategy(strategyPaths.trim().split(",")[0]));
			case "HASH":
				return (new HashedJSONPathsStrategy(strategyPaths.trim().split(",")));
			case "UUID":
				return (new DefaultStrategy());
			case "KAFKA_META_WITH_SLASH":
				return (new KafkaMetaStrategy());
			case "KAFKA_META_HASHED":
				return (new HashedKafkaMetaStrategy());
			default: 
				return (new DefaultStrategy());
		}
	}
	
} 
