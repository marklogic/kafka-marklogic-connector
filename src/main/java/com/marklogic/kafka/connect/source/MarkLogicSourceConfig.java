package com.marklogic.kafka.connect.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * Defines configuration properties for the MarkLogic source connector.
 */
public class MarkLogicSourceConfig extends AbstractConfig {

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
    public static final String ENABLE_CUSTOM_SSL = "ml.connection.enableCustomSsl";
    public static final String TLS_VERSION = "ml.connection.customSsl.tlsVersion";
    public static final String SSL_HOST_VERIFIER = "ml.connection.customSsl.hostNameVerifier";
    public static final String SSL_MUTUAL_AUTH = "ml.connection.customSsl.mutualAuth";

    public static final String DMSDK_BATCH_SIZE = "ml.dmsdk.batchSize";
    public static final String DMSDK_THREAD_COUNT = "ml.dmsdk.threadCount";

    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(CONNECTION_HOST, Type.STRING, Importance.HIGH,
            "Required; a MarkLogic host to connect to. By default, the connector uses the Data Movement SDK, and thus it will connect to each of the hosts in a cluster.")
        .define(CONNECTION_PORT, Type.INT, Importance.HIGH,
            "Required; the port of a REST API app server to connect to")
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
            "Name of a database to connect to. If your REST API server has a content database matching that of the one that you want to read documents from, you do not need to set this.")
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

        .define(DMSDK_BATCH_SIZE, Type.INT, 100, Importance.MEDIUM,
            "Sets the number of documents to be read in a batch from MarkLogic.")
        .define(DMSDK_THREAD_COUNT, Type.INT, 8, Importance.MEDIUM,
            "Sets the number of threads used for parallel reads from MarkLogic.");

    public MarkLogicSourceConfig(final Map<?, ?> originals) {
        super(CONFIG_DEF, originals, false);
    }

}
