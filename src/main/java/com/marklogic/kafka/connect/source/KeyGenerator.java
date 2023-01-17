package com.marklogic.kafka.connect.source;

import java.util.Map;

/**
 * Defines a strategy for generating a key for each source record. Implementations are NOT expected to be thread-safe
 * nor reusable - the intent is that for each poll call in the source connector that returns one or more source
 * records, a new key generator will be constructed.
 */
public interface KeyGenerator {

    /**
     * Generate a new key, which currently is not based on a particular source record. As the need arises - for example,
     * if we choose to support a strategy for generating a key on a particular column value - arguments can be added
     * to this method.
     *
     * @return
     */
    String generateKey();

    /**
     * Factory method, with an unfortunate small leaky abstraction - one implementation requires the MarkLogic
     * server timestamp.
     *
     * @param parsedConfig    the connector configuration
     * @param serverTimestamp the MarkLogic server timestamp associated with the set of rows returned by the call to MarkLogic
     * @return
     */
    static KeyGenerator newKeyGenerator(Map<String, Object> parsedConfig, long serverTimestamp) {
        Object value = parsedConfig.get(MarkLogicSourceConfig.KEY_STRATEGY);
        final String keyStrategy = value != null ? value.toString().toUpperCase() : "";
        switch (keyStrategy) {
            case "UUID":
                return new UuidKeyGenerator();
            case "TIMESTAMP":
                return new TimestampKeyGenerator(serverTimestamp);
            default:
                return new NullKeyGenerator();
        }
    }
}
