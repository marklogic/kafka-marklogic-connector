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
    public static final String JOB_NAME = "ml.source.optic.jobName";
    public static final String CONSISTENT_SNAPSHOT = "ml.source.optic.consistentSnapshot";
    public static final String TOPIC = "ml.source.topic";
    public static final String WAIT_TIME = "ml.source.waitTime";

    public static final String DMSDK_BATCH_SIZE = "ml.dmsdk.batchSize";
    public static final String DMSDK_THREAD_COUNT = "ml.dmsdk.threadCount";

    public static final ConfigDef CONFIG_DEF = getConfigDef();

    private static ConfigDef getConfigDef() {
        ConfigDef configDef = new ConfigDef()
            .define(DSL_PLAN, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.CompositeValidator.of(new ConfigDef.NonNullValidator(), new ConfigDef.NonEmptyString()), Importance.HIGH,
                "Required; a MarkLogic Optic DSL plan for querying the database")
            .define(JOB_NAME, Type.STRING, "", new ConfigDef.NonNullValidator(), Importance.MEDIUM,
                "Specifies the name for the query job executed by the connector task")
            .define(CONSISTENT_SNAPSHOT, Type.BOOLEAN, true, new BooleanValidator(), Importance.MEDIUM,
                "Controls whether to get an immutable view of the result set")
            .define(TOPIC, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.CompositeValidator.of(new ConfigDef.NonNullValidator(), new ConfigDef.NonEmptyString()), Importance.HIGH,
                "Required; the name of the target topic to publish records to")
            .define(WAIT_TIME, Type.LONG, 5000, ConfigDef.Range.atLeast(0), Importance.MEDIUM,
                "Sets the minimum time (in ms) between polling operations")

            .define(DMSDK_BATCH_SIZE, Type.INT, 100, ConfigDef.Range.atLeast(1), Importance.MEDIUM,
                "Sets the number of documents to be read in a batch from MarkLogic")
            .define(DMSDK_THREAD_COUNT, Type.INT, 8, ConfigDef.Range.atLeast(1), Importance.MEDIUM,
                "Sets the number of threads used for parallel reads from MarkLogic");
        MarkLogicConfig.addDefinitions(configDef);
        return configDef;
    }

    private MarkLogicSourceConfig(final Map<?, ?> originals) {
        super(CONFIG_DEF, originals, false);
    }

    static public class BooleanValidator implements ConfigDef.Validator {
        public void ensureValid(String name, Object value) {
            if (!(value instanceof Boolean)) {
                throw new ConfigException(name + " must be either 'true' or 'false'");
            }
        }
    }
}
