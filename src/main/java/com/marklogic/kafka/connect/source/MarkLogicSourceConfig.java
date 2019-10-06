package com.marklogic.kafka.connect.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MarkLogicSourceConfig extends AbstractConfig {

    static final String CONNECTION_HOST = "ml.connection.host";
    static final String QUERY = "ml.query";
    static final String THREAD_COUNT = "ml.dmsdk.threadCount";
    static final String KAFKA_TOPIC = "topic";

    public static ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONNECTION_HOST, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "MarkLogic server hostname")
            .define(QUERY, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "DMSDK threads")
            .define(THREAD_COUNT, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "MarkLogic query")
            .define(KAFKA_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka topic name");

    public MarkLogicSourceConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals, false);
    }
}
