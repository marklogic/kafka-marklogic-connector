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
    public static final String TOPIC = "ml.source.topic";
    public static final String WAIT_TIME = "ml.source.waitTime";

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
                "TBD, we're changing this soon");
    }

    private MarkLogicSourceConfig(final Map<?, ?> originals) {
        super(CONFIG_DEF, originals, false);
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

