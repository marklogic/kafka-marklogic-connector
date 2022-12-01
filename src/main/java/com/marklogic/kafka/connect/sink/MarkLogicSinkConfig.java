package com.marklogic.kafka.connect.sink;

import com.marklogic.kafka.connect.MarkLogicConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * Defines configuration properties for the MarkLogic sink connector.
 */
public class MarkLogicSinkConfig extends MarkLogicConfig {

    public static final String DATAHUB_FLOW_NAME = "ml.datahub.flow.name";
    public static final String DATAHUB_FLOW_STEPS = "ml.datahub.flow.steps";
    public static final String DATAHUB_FLOW_LOG_RESPONSE = "ml.datahub.flow.logResponse";

    public static final String DMSDK_BATCH_SIZE = "ml.dmsdk.batchSize";
    public static final String DMSDK_THREAD_COUNT = "ml.dmsdk.threadCount";
    public static final String DMSDK_TRANSFORM = "ml.dmsdk.transform";
    public static final String DMSDK_TRANSFORM_PARAMS = "ml.dmsdk.transformParams";
    public static final String DMSDK_TRANSFORM_PARAMS_DELIMITER = "ml.dmsdk.transformParamsDelimiter";
    public static final String DMSDK_INCLUDE_KAFKA_METADATA = "ml.dmsdk.includeKafkaMetadata";

    public static final String BULK_DS_ENDPOINT_URI = "ml.sink.bulkds.endpointUri";
    public static final String BULK_DS_BATCH_SIZE = "ml.sink.bulkds.batchSize";

    public static final String DOCUMENT_COLLECTIONS_ADD_TOPIC = "ml.document.addTopicToCollections";
    public static final String DOCUMENT_COLLECTIONS = "ml.document.collections";
    public static final String DOCUMENT_TEMPORAL_COLLECTION = "ml.document.temporalCollection";
    public static final String DOCUMENT_PERMISSIONS = "ml.document.permissions";
    public static final String DOCUMENT_FORMAT = "ml.document.format";
    public static final String DOCUMENT_MIMETYPE = "ml.document.mimeType";
    public static final String DOCUMENT_URI_PREFIX = "ml.document.uriPrefix";
    public static final String DOCUMENT_URI_SUFFIX = "ml.document.uriSuffix";

    public static final String LOGGING_RECORD_KEY = "ml.log.record.key";
    public static final String LOGGING_RECORD_HEADERS = "ml.log.record.headers";

    public static final String ID_STRATEGY = "ml.id.strategy";
    public static final String ID_STRATEGY_PATH = "ml.id.strategy.paths";

    public static final ConfigDef CONFIG_DEF = getConfigDef();

    private static ConfigDef getConfigDef() {
        ConfigDef configDef = new ConfigDef();
        MarkLogicConfig.addDefinitions(configDef);
        return configDef
            .define(BULK_DS_ENDPOINT_URI, Type.STRING, null, Importance.LOW,
                "Defines the URI of a Bulk Data Services endpoint for writing data. " +
                    "See the user guide for more information on using Bulk Data Services instead of DMSDK for writing data to MarkLogic.")
            .define(BULK_DS_BATCH_SIZE, Type.INT, 100, Importance.LOW,
                "Sets the number of documents to be sent in a batch to the Bulk Data Services endpoint. The connector will not send any " +
                    "documents to MarkLogic until it has a number matching this property or until Kafka invokes the 'flush' operation on the connector.")

            .define(DOCUMENT_FORMAT, Type.STRING, null, Importance.MEDIUM,
                "Specify the format of each document; either 'JSON', 'XML', 'BINARY', 'TEXT', or 'UNKNOWN'. If not set, MarkLogic will determine the document type based on the ml.document.uriSuffix property.")
            .define(DOCUMENT_COLLECTIONS, Type.STRING, null, Importance.MEDIUM,
                "Comma-separated list of collections that each document should be written to")
            .define(DOCUMENT_PERMISSIONS, Type.STRING, null, Importance.MEDIUM,
                "Comma-separated list of roles and capabilities that define the permissions for each document")
            .define(DOCUMENT_URI_PREFIX, Type.STRING, null, Importance.MEDIUM,
                "Prefix to prepend to each URI; the URI itself is a UUID")
            .define(DOCUMENT_URI_SUFFIX, Type.STRING, null, Importance.MEDIUM,
                "Suffix to append to each URI; will determine the document type if ml.document.format is not set")
            .define(DOCUMENT_TEMPORAL_COLLECTION, Type.STRING, null, Importance.MEDIUM,
                "Name of a temporal collection for documents to be inserted into")
            .define(DOCUMENT_COLLECTIONS_ADD_TOPIC, Type.BOOLEAN, null, Importance.MEDIUM,
                "Set this to true so that the name of the topic that the connector reads from is added as a collection to each document inserted by the connector")
            .define(DOCUMENT_MIMETYPE, Type.STRING, null, Importance.LOW,
                "Specify a mime type for each document; typically ml.document.format will be used instead of this")

            .define(ID_STRATEGY, Type.STRING, null, Importance.LOW,
                "Set the strategy for generating a unique URI for each document written to MarkLogic. Defaults to 'UUID'. Other choices are: 'JSONPATH', 'HASH', 'KAFKA_META_HASHED', and 'KAFKA_META_WITH_SLASH'.")
            .define(ID_STRATEGY_PATH, Type.STRING, "", Importance.LOW,
                "For use with 'JSONPATH' and 'HASH'; comma-separated list of paths for extracting values for the ID")

            .define(DMSDK_BATCH_SIZE, Type.INT, 100, Importance.MEDIUM,
                "Sets the number of documents to be written in a batch to MarkLogic. This may not have any impact depending on how the connector receives data from Kafka. The connector calls flushAsync on the DMSDK WriteBatcher after processing every collection of records. Thus, if the connector never receives at one time more than the value of this property, then the value of this property will have no impact.")
            .define(DMSDK_THREAD_COUNT, Type.INT, 8, Importance.MEDIUM,
                "Sets the number of threads used for parallel writes to MarkLogic. Similar to the batch size property above, this may never come into play depending on how many records the connector receives at once.")
            .define(DMSDK_TRANSFORM, Type.STRING, null, Importance.MEDIUM,
                "Name of a REST transform to use when writing documents")
            .define(DMSDK_TRANSFORM_PARAMS, Type.STRING, null, Importance.MEDIUM,
                "Delimited set of transform parameter names and values; example = param1,value1,param2,value2")
            .define(DMSDK_TRANSFORM_PARAMS_DELIMITER, Type.STRING, ",", Importance.LOW,
                "Delimiter for transform parameter names and values")
            .define(DMSDK_INCLUDE_KAFKA_METADATA, Type.BOOLEAN, null, Importance.LOW,
                "Set to true so that Kafka record metadata is added to document metadata before it is written. If the document fails to be written, the Kafka record metadata will be logged as well.")

            .define(LOGGING_RECORD_KEY, Type.BOOLEAN, null, Importance.LOW,
                "Set to true to log at the info level the key of each record")
            .define(LOGGING_RECORD_HEADERS, Type.BOOLEAN, null, Importance.LOW,
                "Set to true to log at the info level the headers of each record")

            .define(DATAHUB_FLOW_NAME, Type.STRING, null, Importance.LOW,
                "Name of a Data Hub Framework flow to run after writing documents")
            .define(DATAHUB_FLOW_STEPS, Type.STRING, null, Importance.LOW,
                "Comma-delimited list of step numbers in a flow to run")
            .define(DATAHUB_FLOW_LOG_RESPONSE, Type.BOOLEAN, null, Importance.LOW,
                "Set to true to log at the info level the response data from running a flow");
    }

    public MarkLogicSinkConfig(final Map<?, ?> originals) {
        super(CONFIG_DEF, originals, false);
    }

}
