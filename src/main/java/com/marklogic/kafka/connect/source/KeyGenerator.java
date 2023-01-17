package com.marklogic.kafka.connect.source;

import java.util.Map;

public interface KeyGenerator {
    String generateKey(long rowNumber);

    static KeyGenerator newKeyGenerator(Map<String, Object> parsedConfig) {
        String keyStrategy = parsedConfig.get(MarkLogicSourceConfig.KEY_STRATEGY).toString().toUpperCase();
        switch (keyStrategy) {
            case "UUID":
                return new UuidKeyGenerator();
            case "TIMESTAMP":
                return new TimestampKeyGenerator();
            default:
                 return new NullKeyGenerator();
        }
    }
}
