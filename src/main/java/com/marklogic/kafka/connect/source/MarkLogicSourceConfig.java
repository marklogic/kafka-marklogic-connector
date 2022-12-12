package com.marklogic.kafka.connect.source;

import com.marklogic.kafka.connect.MarkLogicConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

import static java.lang.String.format;

/**
 * Defines configuration properties for the MarkLogic source connector.
 */
public class MarkLogicSourceConfig extends MarkLogicConfig {

    public static final String DSL_QUERY = "ml.source.optic.dsl";
    public static final String SERIALIZED_QUERY = "ml.source.optic.serialized";
    public static final String JOB_NAME = "ml.source.optic.jobName";
    public static final String CONSISTENT_SNAPSHOT = "ml.source.optic.consistentSnapshot";
    public static final String TOPIC = "ml.source.topic";
    public static final String WAIT_TIME = "ml.source.waitTime";

    public static final String DMSDK_BATCH_SIZE = "ml.dmsdk.batchSize";
    public static final String DMSDK_THREAD_COUNT = "ml.dmsdk.threadCount";

    public static final ConfigDef CONFIG_DEF = getConfigDef();

    private static ConfigDef getConfigDef() {
        ConfigDef configDef = new ConfigDef();
        MarkLogicConfig.addDefinitions(configDef);
        return configDef
            .define(DSL_QUERY, Type.STRING, null, new ConfigDef.NonEmptyString(), Importance.HIGH,
                format("Required (or %s); the Optic DSL query to execute", SERIALIZED_QUERY))
            .define(SERIALIZED_QUERY, Type.STRING, null, new ConfigDef.NonEmptyString(), Importance.HIGH,
                format("Required (or %s); the serialized Optic query to execute", DSL_QUERY))
            .define(TOPIC, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.CompositeValidator.of(new ConfigDef.NonNullValidator(), new ConfigDef.NonEmptyString()), Importance.HIGH,
                "Required; the name of a Kafka topic to send records to")
            .define(WAIT_TIME, Type.LONG, 5000, ConfigDef.Range.atLeast(0), Importance.MEDIUM,
                "TBD, we're changing this soon")
            .define(JOB_NAME, Type.STRING, "", new ConfigDef.NonNullValidator(), Importance.MEDIUM,
                "name for the job run by the connector; the main use case for this is to " +
                    "enhance logging by having a known job name appear in the logs")
            .define(CONSISTENT_SNAPSHOT, Type.BOOLEAN, true, new BooleanValidator(), Importance.MEDIUM,
                "enables retrieval of rows that were present in the view at the time that the " +
                    "first batch is retrieved, ignoring subsequent changes to the view; defaults to true; setting this to false will " +
                    "result in matching rows inserted or updated after the retrieval of the first batch being included as well")
            .define(DMSDK_BATCH_SIZE, Type.INT, 100, ConfigDef.Range.atLeast(1), Importance.MEDIUM,
                "sets the number of rows to be read in a batch from MarkLogic; can adjust this to tune performance")
            .define(DMSDK_THREAD_COUNT, Type.INT, 8, ConfigDef.Range.atLeast(1), Importance.MEDIUM,
                "sets the number of threads to use for reading batches of rows from MarkLogic; can adjust this to tune performance");
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
