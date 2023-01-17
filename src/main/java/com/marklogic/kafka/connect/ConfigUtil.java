package com.marklogic.kafka.connect;

import java.util.Map;

public interface ConfigUtil {

    /**
     * Convenience method for getting a boolean value from the parsed Kafka config, returning false if the given key is
     * not found. This accounts for the fact that boolean options are expected to have "null" as a default value, which
     * ensures in Confluent Platform that the default value is shown as "false". Oddly, if the default value is "false",
     * then Confluent Platform seems to erroneously show the default value as "true".
     *
     * @param key
     * @param parsedConfig
     * @return
     */
    static boolean getBoolean(String key, Map<String, Object> parsedConfig) {
        Boolean val = (Boolean) parsedConfig.get(key);
        if (val == null) {
            return false;
        }
        return val;
    }
}
