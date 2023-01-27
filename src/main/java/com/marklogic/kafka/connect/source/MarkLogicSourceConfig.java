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
    public static final String ROW_LIMIT = "ml.source.optic.rowLimit";
    public static final String CONSTRAINT_COLUMN_NAME = "ml.source.optic.constraintColumn.name";
    public static final String CONSTRAINT_STORAGE_URI = "ml.source.optic.constraintColumn.uri";
    public static final String CONSTRAINT_STORAGE_PERMISSIONS = "ml.source.optic.constraintColumn.permissions";
    public static final String CONSTRAINT_STORAGE_COLLECTIONS = "ml.source.optic.constraintColumn.collections";
    public static final String OUTPUT_FORMAT = "ml.source.optic.outputFormat";
    enum OUTPUT_TYPE {JSON, XML, CSV}
    private static final CustomRecommenderAndValidator OUTPUT_FORMAT_RV =
        new CustomRecommenderAndValidator(Arrays.stream(MarkLogicSourceConfig.OUTPUT_TYPE.values()).map(Enum::toString).toArray(String[]::new));
    public static final String KEY_COLUMN = "ml.source.optic.keyColumn";
    public static final String INCLUDE_COLUMN_TYPES = "ml.source.optic.includeColumnTypes";
    public static final String TOPIC = "ml.source.topic";
    public static final String WAIT_TIME = "ml.source.waitTime";

    public static final ConfigDef CONFIG_DEF = getConfigDef();
    private static final DefaultDocumentPermissionsParser permissionsParser = new DefaultDocumentPermissionsParser();
    private static final String GROUP = "MarkLogic Source Settings";

    private static ConfigDef getConfigDef() {
        ConfigDef configDef = new ConfigDef();
        MarkLogicConfig.addDefinitions(configDef);
        return configDef
            .define(TOPIC, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.CompositeValidator.of(new ConfigDef.NonNullValidator(), new ConfigDef.NonEmptyString()), Importance.HIGH,
                "Required; the name of a Kafka topic to send records to",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Kafka Topic Name")
            .define(DSL_QUERY, Type.STRING, null, Importance.HIGH,
                format("Required (or %s); the Optic DSL query to execute", SERIALIZED_QUERY),
                GROUP, -1, ConfigDef.Width.MEDIUM, "Optic DSL Query")
            .define(SERIALIZED_QUERY, Type.STRING, null, Importance.HIGH,
                format("Required (or %s); the serialized Optic query to execute", DSL_QUERY),
                GROUP, -1, ConfigDef.Width.MEDIUM, "Optic Serialized Query")
            .define(OUTPUT_FORMAT, Type.STRING, "JSON", OUTPUT_FORMAT_RV, Importance.HIGH,
                "The structure of the data in the query response",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Output Format", OUTPUT_FORMAT_RV)
            .define(ROW_LIMIT, Type.INT, 0, ConfigDef.Range.atLeast(0), Importance.MEDIUM,
                "The maximum number of rows to retrieve per poll",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Row Limit")
            .define(WAIT_TIME, Type.LONG, 5000, ConfigDef.Range.atLeast(0), Importance.MEDIUM,
                "Required, must be zero or higher; the amount of time in milliseconds to wait, on each poll call, " +
                    "before querying MarkLogic. Kafka will continually call poll on the source connector, so this " +
                    "can be used to control how frequently the connector queries MarkLogic.",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Wait Time")
            .define(KEY_COLUMN, Type.STRING, null, Importance.MEDIUM,
                "The name of a column to use for creating a key for each source record. Note that the accessor in your Optic " +
                    "query may affect the column names in each row that it returns; for example, fromView will prepend " +
                    "schema and view names to the column. Be sure your key column name matches a column in each row that " +
                    "is returned.",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Key Column")
            .define(INCLUDE_COLUMN_TYPES, Type.BOOLEAN, null, Importance.MEDIUM,
                "Set to true for column types to be included in the value of each source record; has no effect if the output " +
                    "format is CSV",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Include Column Types")
            .define(CONSTRAINT_COLUMN_NAME, Type.STRING, null, Importance.HIGH,
                "The name of the column which should be used to constrain the Optic query; typically used when only " +
                    "new or modified data should be returned.",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Constraint Column Name")
            .define(CONSTRAINT_STORAGE_URI, Type.STRING, null, Importance.MEDIUM,
                "The URI of the JSON document in MarkLogic used to store constraint value information. " +
                    "Since it is JSON data, it is recommended that the URI have a '.json' suffix.",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Constraint Storage URI")
            .define(CONSTRAINT_STORAGE_PERMISSIONS, Type.STRING, null, new PermissionsValidator(), Importance.MEDIUM,
                "Comma-separated list of roles and capabilities that define the permissions for the constraint value document",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Constraint Storage Permissions")
            .define(CONSTRAINT_STORAGE_COLLECTIONS, Type.STRING, null, Importance.MEDIUM,
                "Comma-separated list of collections to assign to the constraint value document",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Constraint Storage Collections");
    }

    public MarkLogicSourceConfig(final Map<?, ?> originals) {
        super(CONFIG_DEF, originals, false);
    }

    public static class PermissionsValidator implements ConfigDef.Validator {
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

