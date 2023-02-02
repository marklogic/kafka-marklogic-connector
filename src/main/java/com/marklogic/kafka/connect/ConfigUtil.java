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
