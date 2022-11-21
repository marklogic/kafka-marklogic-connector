package com.marklogic.kafka.connect.source;

import com.marklogic.kafka.connect.MarkLogicConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

/**
 * Defines configuration properties for the MarkLogic source connector.
 */
public class MarkLogicSourceConfig extends MarkLogicConfig {

    public static final String DSL_PLAN = "ml.source.optic.dsl";

    public static final String DMSDK_BATCH_SIZE = "ml.dmsdk.batchSize";
    public static final String DMSDK_THREAD_COUNT = "ml.dmsdk.threadCount";

    public static final ConfigDef CONFIG_DEF = getConfigDef();

    private static ConfigDef getConfigDef() {
        ConfigDef configDef = new ConfigDef()
            .define(DSL_PLAN, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new FreeFormStringValidator(), Importance.HIGH,
                "Required; a MarkLogic Optic DSL plan for querying the database")

            .define(DMSDK_BATCH_SIZE, Type.INT, 100, Importance.MEDIUM,
                "Sets the number of documents to be read in a batch from MarkLogic")
            .define(DMSDK_THREAD_COUNT, Type.INT, 8, Importance.MEDIUM,
                "Sets the number of threads used for parallel reads from MarkLogic");
        MarkLogicConfig.addDefinitions(configDef);
        return configDef;
    }

    private MarkLogicSourceConfig(final Map<?, ?> originals) {
        super(CONFIG_DEF, originals, false);
    }

    static public class FreeFormStringValidator implements ConfigDef.Validator {
        public void ensureValid(String name, Object value) {
            if (value == null) {
                throw new ConfigException(name + " must not be null");
            }
            if (!(value instanceof String)) {
                throw new ConfigException(name + " must be a String");
            }
            if (((String) value).length() == 0) {
                throw new ConfigException(name + " must not be an empty String");
            }
        }
    }
}
