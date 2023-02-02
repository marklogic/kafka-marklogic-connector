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

    private static final CustomRecommenderAndValidator DOCUMENT_FORMAT_RV = new CustomRecommenderAndValidator("JSON", "XML", "BINARY", "TEXT", "UNKNOWN", "");
    private static final CustomRecommenderAndValidator ID_STRATEGY_RV = new CustomRecommenderAndValidator("JSONPATH", "HASH", "KAFKA_META_HASHED", "KAFKA_META_WITH_SLASH", "");

    public static final ConfigDef CONFIG_DEF = getConfigDef();

    private static final String GROUP = "MarkLogic Sink Settings";

    private static ConfigDef getConfigDef() {
        ConfigDef configDef = new ConfigDef();
        MarkLogicConfig.addDefinitions(configDef);

        return configDef
            .define(DOCUMENT_FORMAT, Type.STRING, "", DOCUMENT_FORMAT_RV, Importance.MEDIUM,
                "Specify the format of each document; either 'JSON', 'XML', 'BINARY', 'TEXT', or 'UNKNOWN'. If not set, MarkLogic will determine the document type based on the ml.document.uriSuffix property.",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Document Format", DOCUMENT_FORMAT_RV)
            .define(DOCUMENT_COLLECTIONS, Type.STRING, null, Importance.MEDIUM,
                "Comma-separated list of collections that each document should be written to",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Collections")
            .define(DOCUMENT_PERMISSIONS, Type.STRING, null, Importance.MEDIUM,
                "Comma-separated list of roles and capabilities that define the permissions for each document",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Permissions")
            .define(DOCUMENT_URI_PREFIX, Type.STRING, null, Importance.MEDIUM,
                "Prefix to prepend to each URI; the URI itself is a UUID",
                GROUP, -1, ConfigDef.Width.MEDIUM, "URI Prefix")
            .define(DOCUMENT_URI_SUFFIX, Type.STRING, null, Importance.MEDIUM,
                "Suffix to append to each URI; will determine the document type if ml.document.format is not set",
                GROUP, -1, ConfigDef.Width.MEDIUM, "URI Suffix")
            .define(DOCUMENT_TEMPORAL_COLLECTION, Type.STRING, null, Importance.MEDIUM,
                "Name of a temporal collection for documents to be inserted into",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Temporal Collection")
            .define(DOCUMENT_COLLECTIONS_ADD_TOPIC, Type.BOOLEAN, null, Importance.MEDIUM,
                "Set this to true so that the name of the topic that the connector reads from is added as a collection to each document inserted by the connector",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Add Topic as Collection")
            .define(DOCUMENT_MIMETYPE, Type.STRING, null, Importance.LOW,
                "Specify a mime type for each document; typically ml.document.format will be used instead of this",
                GROUP, -1, ConfigDef.Width.MEDIUM, "MIME Type")

            .define(ID_STRATEGY, Type.STRING, "", ID_STRATEGY_RV, Importance.LOW,
                "Set the strategy for generating a unique URI for each document written to MarkLogic. Defaults to 'UUID'. Other choices are: 'JSONPATH', 'HASH', 'KAFKA_META_HASHED', and 'KAFKA_META_WITH_SLASH'.",
                GROUP, -1, ConfigDef.Width.SHORT, "ID Strategy for URI", ID_STRATEGY_RV)
            .define(ID_STRATEGY_PATH, Type.STRING, "", Importance.LOW,
                "For use with 'JSONPATH' and 'HASH'; comma-separated list of paths for extracting values for the ID",
                GROUP, -1, ConfigDef.Width.MEDIUM, "ID Strategy Path")

            .define(DMSDK_BATCH_SIZE, Type.INT, 100, ConfigDef.Range.atLeast(1), Importance.MEDIUM,
                "Sets the number of documents to be written in a batch to MarkLogic. This may not have any impact depending on how the connector receives data from Kafka. The connector calls flushAsync on the DMSDK WriteBatcher after processing every collection of records. Thus, if the connector never receives at one time more than the value of this property, then the value of this property will have no impact.",
                GROUP, -1, ConfigDef.Width.MEDIUM, "DMSDK Batch Size")
            .define(DMSDK_THREAD_COUNT, Type.INT, 8, ConfigDef.Range.atLeast(1), Importance.MEDIUM,
                "Sets the number of threads used for parallel writes to MarkLogic. Similar to the batch size property above, this may never come into play depending on how many records the connector receives at once.",
                GROUP, -1, ConfigDef.Width.MEDIUM, "DMSDK Thread Count")
            .define(DMSDK_TRANSFORM, Type.STRING, null, Importance.MEDIUM,
                "Name of a REST transform to use when writing documents",
                GROUP, -1, ConfigDef.Width.MEDIUM, "DMSDK Transform")
            .define(DMSDK_TRANSFORM_PARAMS, Type.STRING, null, Importance.MEDIUM,
                "Delimited set of transform parameter names and values; example = param1,value1,param2,value2",
                GROUP, -1, ConfigDef.Width.MEDIUM, "DMSDK Transform Parameters")
            .define(DMSDK_TRANSFORM_PARAMS_DELIMITER, Type.STRING, ",", Importance.LOW,
                "Delimiter for transform parameter names and values",
                GROUP, -1, ConfigDef.Width.MEDIUM, "DMSDK Transform Parameters Delimiter")
            .define(DMSDK_INCLUDE_KAFKA_METADATA, Type.BOOLEAN, null, Importance.LOW,
                "Set to true so that Kafka record metadata is added to document metadata before it is written. If the document fails to be written, the Kafka record metadata will be logged as well.",
                GROUP, -1, ConfigDef.Width.MEDIUM, "DMSDK Include Kafka Metadata")

            .define(BULK_DS_ENDPOINT_URI, Type.STRING, null, Importance.LOW,
                "Defines the URI of a Bulk Data Services endpoint for writing data. " +
                    "See the user guide for more information on using Bulk Data Services instead of DMSDK for writing data to MarkLogic.",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Bulk Data Services Endpoint URI")
            .define(BULK_DS_BATCH_SIZE, Type.INT, 100, ConfigDef.Range.atLeast(1), Importance.LOW,
                "Sets the number of documents to be sent in a batch to the Bulk Data Services endpoint. The connector will not send any documents to MarkLogic until it " +
                    "has a number matching this property or until Kafka invokes the 'flush' operation on the connector.",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Bulk Data Services Batch Size")

            .define(LOGGING_RECORD_KEY, Type.BOOLEAN, null, Importance.LOW,
                "Set to true to log at the info level the key of each record",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Log Record Keys")
            .define(LOGGING_RECORD_HEADERS, Type.BOOLEAN, null, Importance.LOW,
                "Set to true to log at the info level the headers of each record",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Log Record Headers")

            .define(DATAHUB_FLOW_NAME, Type.STRING, null, Importance.LOW,
                "Name of a Data Hub Framework flow to run after writing documents",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Data Hub Flow Name")
            .define(DATAHUB_FLOW_STEPS, Type.STRING, null, Importance.LOW,
                "Comma-delimited list of step numbers in a flow to run",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Data Hub Flow Steps")
            .define(DATAHUB_FLOW_LOG_RESPONSE, Type.BOOLEAN, null, Importance.LOW,
                "Set to true to log at the info level the response data from running a flow",
                GROUP, -1, ConfigDef.Width.MEDIUM, "Log Data Hub Flow Response");
    }

    public MarkLogicSinkConfig(final Map<?, ?> originals) {
        super(CONFIG_DEF, originals, false);
    }
}
