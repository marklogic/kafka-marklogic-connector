package com.marklogic.kafka.connect.source;

import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.kafka.connect.MarkLogicConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Map;

import static java.lang.String.format;

/**
 * Defines configuration properties for the MarkLogic source connector.
 */
public class MarkLogicSourceConfig extends MarkLogicConfig {

    public static final String DSL_QUERY = "ml.source.optic.dsl";
    public static final String SERIALIZED_QUERY = "ml.source.optic.serialized";
    public static final String CONSTRAINT_COLUMN_NAME = "ml.source.optic.constraintColumn.name";
    public static final String CONSTRAINT_STORAGE_URI = "ml.source.optic.constraintColumn.uri";
    public static final String CONSTRAINT_STORAGE_PERMISSIONS = "ml.source.optic.constraintColumn.permissions";
    public static final String CONSTRAINT_STORAGE_COLLECTIONS = "ml.source.optic.constraintColumn.collections";
    public static final String OUTPUT_FORMAT = "ml.source.optic.outputFormat";
    enum OUTPUT_TYPE {JSON, XML, CSV}
    private static final CustomRecommenderAndValidator OUTPUT_FORMAT_RV =
        new CustomRecommenderAndValidator(Arrays.stream(MarkLogicSourceConfig.OUTPUT_TYPE.values()).map(Enum::toString).toArray(String[]::new));
    public static final String JOB_NAME = "ml.source.optic.jobName";
    public static final String CONSISTENT_SNAPSHOT = "ml.source.optic.consistentSnapshot";
    public static final String TOPIC = "ml.source.topic";
    public static final String WAIT_TIME = "ml.source.waitTime";

    public static final String DMSDK_BATCH_SIZE = "ml.dmsdk.batchSize";
    public static final String DMSDK_THREAD_COUNT = "ml.dmsdk.threadCount";

    public static final ConfigDef CONFIG_DEF = getConfigDef();
    private static final DefaultDocumentPermissionsParser permissionsParser = new DefaultDocumentPermissionsParser();


    private static ConfigDef getConfigDef() {
        ConfigDef configDef = new ConfigDef();
        MarkLogicConfig.addDefinitions(configDef);
        return configDef
            .define(DSL_QUERY, Type.STRING, null, Importance.HIGH,
                format("Required (or %s); The Optic DSL query to execute", SERIALIZED_QUERY))
            .define(SERIALIZED_QUERY, Type.STRING, null, Importance.HIGH,
                format("Required (or %s); The serialized Optic query to execute", DSL_QUERY))
            .define(CONSTRAINT_COLUMN_NAME, Type.STRING, null, Importance.HIGH,
                "The name of the column which should be used to constrain the Optic query")
            .define(CONSTRAINT_STORAGE_URI, Type.STRING, null, Importance.MEDIUM,
                "The URI of the JSON document in MarkLogic used to store constraint value information. " +
                    "Since it is JSON data, the URI should have a '.json' suffix.")
            .define(CONSTRAINT_STORAGE_PERMISSIONS, Type.STRING, null, new PermissionsValidator(), Importance.HIGH,
                "Comma-separated list of roles and capabilities that define the permissions for the constraint value document")
            .define(CONSTRAINT_STORAGE_COLLECTIONS, Type.STRING, null, Importance.HIGH,
                "Comma-separated list of collections to assign to the constraint value document")
            .define(OUTPUT_FORMAT, Type.STRING, "JSON", OUTPUT_FORMAT_RV, Importance.HIGH,
                "The structure of the data in the query response", null, -1, ConfigDef.Width.MEDIUM, OUTPUT_FORMAT, OUTPUT_FORMAT_RV)
            .define(TOPIC, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.CompositeValidator.of(new ConfigDef.NonNullValidator(), new ConfigDef.NonEmptyString()), Importance.HIGH,
                "Required; The name of a Kafka topic to send records to")
            .define(WAIT_TIME, Type.LONG, 5000, ConfigDef.Range.atLeast(0), Importance.MEDIUM,
                "TBD, we're changing this soon")
            .define(JOB_NAME, Type.STRING, "", new ConfigDef.NonNullValidator(), Importance.MEDIUM,
                "Name for the job run by the connector; the main use case for this is to " +
                    "enhance logging by having a known job name appear in the logs")
            .define(CONSISTENT_SNAPSHOT, Type.BOOLEAN, true, new BooleanValidator(), Importance.MEDIUM,
                "Enables retrieval of rows that were present in the view at the time that the " +
                    "first batch is retrieved, ignoring subsequent changes to the view; defaults to true; setting this to false will " +
                    "result in matching rows inserted or updated after the retrieval of the first batch being included as well")
            // Batch size is a bit different for RowBatcher than QueryBatcher, as it - along with the row estimate that
            // RowBatcher gathers - is used to determine the number of partitions. The more partitions, the more calls
            // RowBatcher has to make to ML. That's good when there are a lot of matching rows - like hundreds of
            // thousands - but not good when there's a smaller set of matching rows. From initial testing, 10k seems
            // like a reasonable default size for batches that provides good performance when there are hundreds of
            // thousands of matches or more, and also when only a few rows match. Ultimately, it's up a user to perform
            // their own performance testing to determine a suitable batch size.
            .define(DMSDK_BATCH_SIZE, Type.INT, 10000, ConfigDef.Range.atLeast(1), Importance.MEDIUM,
                "Sets the number of rows to be read in a batch from MarkLogic; can adjust this to tune performance")
            .define(DMSDK_THREAD_COUNT, Type.INT, 16, ConfigDef.Range.atLeast(1), Importance.MEDIUM,
                "Sets the number of threads to use for reading batches of rows from MarkLogic; can adjust this to tune performance");
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

    static public class PermissionsValidator implements ConfigDef.Validator {
        public void ensureValid(String name, Object value) {
            if (StringUtils.hasText((String) value)) {
                DocumentMetadataHandle metadata = new DocumentMetadataHandle();
                try {
                    permissionsParser.parsePermissions((String) value, metadata.getPermissions());
                } catch (IllegalArgumentException ex) {
                    throw new ConfigException(ex.getMessage());
                }
            }
        }
    }
}

