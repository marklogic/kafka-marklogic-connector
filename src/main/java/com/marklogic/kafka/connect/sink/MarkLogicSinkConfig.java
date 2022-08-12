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

    public static final String DOCUMENT_COLLECTIONS_ADD_TOPIC = "ml.document.addTopicToCollections";
    public static final String DOCUMENT_COLLECTIONS = "ml.document.collections";
    public static final String DOCUMENT_PERMISSIONS = "ml.document.permissions";
    public static final String DOCUMENT_FORMAT = "ml.document.format";
    public static final String DOCUMENT_MIMETYPE = "ml.document.mimeType";
    public static final String DOCUMENT_URI_PREFIX = "ml.document.uriPrefix";
    public static final String DOCUMENT_URI_SUFFIX = "ml.document.uriSuffix";

    public static final String SSL = "ml.connection.enableCustomSsl";
    public static final String TLS_VERSION = "ml.connection.customSsl.tlsVersion";
    public static final String SSL_HOST_VERIFIER = "ml.connection.customSsl.hostNameVerifier";
    public static final String SSL_MUTUAL_AUTH = "ml.connection.customSsl.mutualAuth";

    public static final String LOGGING_RECORD_KEY = "ml.log.record.key";
    public static final String LOGGING_RECORD_HEADERS = "ml.log.record.headers";

    public static final String ID_STRATEGY = "ml.id.strategy";
    public static final String ID_STRATEGY_PATH = "ml.id.strategy.paths";

    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(CONNECTION_HOST, Type.STRING, Importance.HIGH, "MarkLogic server hostname")
        .define(CONNECTION_PORT, Type.INT, Importance.HIGH, "The REST app server port to connect to")
        .define(CONNECTION_DATABASE, Type.STRING, "", Importance.LOW, "Database to connect, if different from the one associated with the port")
        .define(CONNECTION_SECURITY_CONTEXT_TYPE, Type.STRING, "NONE", Importance.HIGH, "Type of MarkLogic security context to create - either digest, basic, kerberos, certificate, or none")
        .define(CONNECTION_USERNAME, Type.STRING, Importance.HIGH, "Name of MarkLogic user to authenticate as")
        .define(CONNECTION_PASSWORD, Type.STRING, Importance.HIGH, "Password for the MarkLogic user")
        .define(CONNECTION_TYPE, Type.STRING, "DIRECT", Importance.LOW, "Connection type; DIRECT or GATEWAY")
        .define(CONNECTION_SIMPLE_SSL, Type.BOOLEAN, false, Importance.LOW, "Set to true to use a trust-everything SSL connection")
        .define(CONNECTION_CERT_FILE, Type.STRING, "", Importance.LOW, "Path to a certificate file")
        .define(CONNECTION_CERT_PASSWORD, Type.STRING, "", Importance.LOW, "Password for the certificate file")
        .define(CONNECTION_EXTERNAL_NAME, Type.STRING, "", Importance.LOW, "External name for Kerberos authentication")
        .define(DATAHUB_FLOW_NAME, Type.STRING, null, Importance.MEDIUM, "Name of a Data Hub flow to run")
        .define(DATAHUB_FLOW_STEPS, Type.STRING, null, Importance.MEDIUM, "Comma-delimited names of steps to run")
        .define(DATAHUB_FLOW_LOG_RESPONSE, Type.BOOLEAN, false, Importance.LOW, "If set to true, the response from running a flow on each ingested batch will be logged at the info level")
        .define(DMSDK_BATCH_SIZE, Type.INT, 100, Importance.HIGH, "Number of documents to write in each batch")
        .define(DMSDK_THREAD_COUNT, Type.INT, 8, Importance.HIGH, "Number of threads for DMSDK to use")
        .define(DMSDK_TRANSFORM, Type.STRING, "", Importance.MEDIUM, "Name of a REST transform to use when writing documents")
        .define(DMSDK_TRANSFORM_PARAMS, Type.STRING, "", Importance.MEDIUM, "Delimited set of transform names and values")
        .define(DMSDK_TRANSFORM_PARAMS_DELIMITER, Type.STRING, ",", Importance.LOW, "Delimiter for transform parameter names and values; defaults to a comma")
        .define(DOCUMENT_COLLECTIONS_ADD_TOPIC, Type.BOOLEAN, false, Importance.LOW, "Indicates if the topic name should be added to the set of collections for a document")
        .define(DOCUMENT_COLLECTIONS, Type.STRING, "", Importance.MEDIUM, "String-delimited collections to add each document to")
        .define(DOCUMENT_FORMAT, Type.STRING, "", Importance.LOW, "Defines format of each document; can be one of json, xml, text, binary, or unknown")
        .define(DOCUMENT_MIMETYPE, Type.STRING, "", Importance.LOW, "Defines the mime type of each document; optional, and typically the format is set instead of the mime type")
        .define(DOCUMENT_PERMISSIONS, Type.STRING, "", Importance.MEDIUM, "String-delimited permissions to add to each document; role1,capability1,role2,capability2,etc")
        .define(DOCUMENT_URI_PREFIX, Type.STRING, "", Importance.MEDIUM, "Prefix to prepend to each generated URI")
        .define(DOCUMENT_URI_SUFFIX, Type.STRING, "", Importance.MEDIUM, "Suffix to append to each generated URI")
        .define(SSL, Type.BOOLEAN, false, Importance.LOW, "Whether SSL connection to the App server - true or false.")
        .define(TLS_VERSION, Type.STRING, "", Importance.LOW, "Version of TLS to connect to MarkLogic SSL enabled App server. Ex. TLSv1.2")
        .define(SSL_HOST_VERIFIER, Type.STRING, "", Importance.LOW, "The strictness of Host Verifier - ANY, COMMON, STRICT")
        .define(SSL_MUTUAL_AUTH, Type.BOOLEAN, false, Importance.LOW, "Mutual Authentication for Basic or Digest : true or false")

        .define(LOGGING_RECORD_KEY, Type.BOOLEAN, false, Importance.LOW, "Log incoming record keys")
        .define(LOGGING_RECORD_HEADERS, Type.BOOLEAN, false, Importance.LOW, "Log incoming record headers")

        .define(ID_STRATEGY, Type.STRING, "", Importance.LOW, "The ID Strategy for URI.")
        .define(ID_STRATEGY_PATH, Type.STRING, "", Importance.LOW, "The JSON path for ID Strategy");

    public MarkLogicSinkConfig(final Map<?, ?> originals) {
        super(CONFIG_DEF, originals, false);
    }

}
