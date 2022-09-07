package com.marklogic.kafka.connect.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * Defines configuration properties for the MarkLogic sink connector.
 */
public class MarkLogicSinkConfig extends AbstractConfig {

    public static final String CONNECTION_HOST = "ml.connection.host";
    public static final String CONNECTION_PORT = "ml.connection.port";
    public static final String CONNECTION_DATABASE = "ml.connection.database";
    public static final String CONNECTION_MODULES_DATABASE = "ml.connection.modulesDatabase";
    public static final String CONNECTION_SECURITY_CONTEXT_TYPE = "ml.connection.securityContextType";
    public static final String CONNECTION_USERNAME = "ml.connection.username";
    public static final String CONNECTION_PASSWORD = "ml.connection.password";
    public static final String CONNECTION_TYPE = "ml.connection.type";
    public static final String CONNECTION_SIMPLE_SSL = "ml.connection.simpleSsl";
    public static final String CONNECTION_CERT_FILE = "ml.connection.certFile";
    public static final String CONNECTION_CERT_PASSWORD = "ml.connection.certPassword";
    public static final String CONNECTION_EXTERNAL_NAME = "ml.connection.externalName";

    public static final String DATAHUB_FLOW_NAME = "ml.datahub.flow.name";
    public static final String DATAHUB_FLOW_STEPS = "ml.datahub.flow.steps";
    public static final String DATAHUB_FLOW_LOG_RESPONSE = "ml.datahub.flow.logResponse";

    public static final String DMSDK_BATCH_SIZE = "ml.dmsdk.batchSize";
    public static final String DMSDK_THREAD_COUNT = "ml.dmsdk.threadCount";
    public static final String DMSDK_TRANSFORM = "ml.dmsdk.transform";
    public static final String DMSDK_TRANSFORM_PARAMS = "ml.dmsdk.transformParams";
    public static final String DMSDK_TRANSFORM_PARAMS_DELIMITER = "ml.dmsdk.transformParamsDelimiter";
    public static final String DMSDK_INCLUDE_KAFKA_METADATA = "ml.dmsdk.includeKafkaMetadata";

    public static final String BULK_DS_API_URI = "ml.sink.bulkds.apiUri";

    public static final String DOCUMENT_COLLECTIONS_ADD_TOPIC = "ml.document.addTopicToCollections";
    public static final String DOCUMENT_COLLECTIONS = "ml.document.collections";
    public static final String DOCUMENT_TEMPORAL_COLLECTION = "ml.document.temporalCollection";
    public static final String DOCUMENT_PERMISSIONS = "ml.document.permissions";
    public static final String DOCUMENT_FORMAT = "ml.document.format";
    public static final String DOCUMENT_MIMETYPE = "ml.document.mimeType";
    public static final String DOCUMENT_URI_PREFIX = "ml.document.uriPrefix";
    public static final String DOCUMENT_URI_SUFFIX = "ml.document.uriSuffix";

    public static final String ENABLE_CUSTOM_SSL = "ml.connection.enableCustomSsl";
    public static final String TLS_VERSION = "ml.connection.customSsl.tlsVersion";
    public static final String SSL_HOST_VERIFIER = "ml.connection.customSsl.hostNameVerifier";
    public static final String SSL_MUTUAL_AUTH = "ml.connection.customSsl.mutualAuth";

    public static final String LOGGING_RECORD_KEY = "ml.log.record.key";
    public static final String LOGGING_RECORD_HEADERS = "ml.log.record.headers";

    public static final String ID_STRATEGY = "ml.id.strategy";
    public static final String ID_STRATEGY_PATH = "ml.id.strategy.paths";

    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(CONNECTION_HOST, Type.STRING, Importance.HIGH,
            "Required; a MarkLogic host to connect to. By default, the connector uses the Data Movement SDK, and thus it will connect to each of the hosts in a cluster.")
        .define(CONNECTION_PORT, Type.INT, Importance.HIGH,
            "Required; the port of a REST API app server to connect to; if using Bulk Data Services, can be a plain HTTP app server")
        .define(CONNECTION_SECURITY_CONTEXT_TYPE, Type.STRING, "DIGEST", Importance.HIGH,
            "Required; the authentication scheme used by the server defined by ml.connection.port; either 'DIGEST', 'BASIC', 'CERTIFICATE', 'KERBEROS', or 'NONE'")
        .define(CONNECTION_USERNAME, Type.STRING, null, Importance.MEDIUM,
            "MarkLogic username for 'DIGEST' and 'BASIC' authentication")
        .define(CONNECTION_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM,
            "MarkLogic password for 'DIGEST' and 'BASIC' authentication")
        .define(CONNECTION_CERT_FILE, Type.STRING, null, Importance.MEDIUM,
            "Path to PKCS12 file for 'CERTIFICATE' authentication")
        .define(CONNECTION_CERT_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM,
            "Password for PKCS12 file for 'CERTIFICATE' authentication")
        .define(CONNECTION_EXTERNAL_NAME, Type.STRING, null, Importance.MEDIUM,
            "External name for 'KERBEROS' authentication")
        .define(CONNECTION_DATABASE, Type.STRING, null, Importance.LOW,
            "Name of a database to connect to. If your REST API server has a content database matching that of the one that you want to write documents to, you do not need to set this.")
        .define(CONNECTION_MODULES_DATABASE, Type.STRING, null, Importance.MEDIUM,
            "Name of the modules database associated with the app server; required if using Bulk Data Services so that the API module can be retrieved")
        .define(CONNECTION_TYPE, Type.STRING, null, Importance.MEDIUM,
            "Set to 'GATEWAY' when the host identified by ml.connection.host is a load balancer. See https://docs.marklogic.com/guide/java/data-movement#id_26583 for more information.")
        // Boolean fields must have a default value of null; otherwise, Confluent Platform, at least in version 7.2.1,
        // will show a default value of "true"
        .define(CONNECTION_SIMPLE_SSL, Type.BOOLEAN, null, Importance.LOW,
            "Set to 'true' for a simple SSL strategy that uses the JVM's default SslContext and X509TrustManager and an 'any' host verification strategy")
        .define(ENABLE_CUSTOM_SSL, Type.BOOLEAN, null, Importance.LOW,
            "Set to 'true' to customize how an SSL connection is created. Only supported if securityContextType is 'BASIC' or 'DIGEST'.")
        .define(TLS_VERSION, Type.STRING, "TLSv1.2", Importance.LOW,
            "The TLS version to use for custom SSL")
        .define(SSL_HOST_VERIFIER, Type.STRING, "ANY", Importance.LOW,
            "The host verification strategy for custom SSL; either 'ANY', 'COMMON', or 'STRICT'")
        .define(SSL_MUTUAL_AUTH, Type.BOOLEAN, null, Importance.LOW,
            "Set this to true for 2-way SSL; defaults to 1-way SSL")

        .define(BULK_DS_API_URI, Type.STRING, null, Importance.LOW,
            "Defines the URI of a Bulk Data Services API declaration. Requires that ml.connection.modulesDatabase be set. See the " +
                "user guide for more information on using Bulk Data Services instead of DMSDK for writing data to MarkLogic.")

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

    public MarkLogicSinkConfig(final Map<?, ?> originals) {
        super(CONFIG_DEF, originals, false);
    }

}
