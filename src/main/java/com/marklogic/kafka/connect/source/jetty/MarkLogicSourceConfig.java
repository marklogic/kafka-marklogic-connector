package com.marklogic.kafka.connect.source.jetty;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * Defines configuration properties for the MarkLogic sink connector.
 */
public class MarkLogicSourceConfig extends AbstractConfig {

	public static final String JETTY_PORT = "jetty.port";
    public static final String JETTY_SECURE = "jetty.secure";
    public static final String JETTY_KEYSTORE_PATH = "jetty.ssl.keystore.path";
    public static final String JETTY_KEYSTORE_PASSWORD = "jetty.ssl.keystore.password";
    public static final String JETTY_KEYSTORE_MANAGER_PASSWORD = "jetty.ssl.keystore.manager.password";
    public static final String JETTY_TRUSTSTORE_PATH = "jetty.ssl.truststore.path";
    public static final String JETTY_TRUSTSTORE_PASSWORD = "jetty.ssl.truststore.password";
    public static final String JETTY_CLIENT_AUTH = "jetty.ssl.client.auth";


	public static ConfigDef CONFIG_DEF = new ConfigDef()
		.define(JETTY_PORT, Type.STRING, Importance.HIGH, "Jetty Server port")
	    .define(JETTY_SECURE, Type.BOOLEAN, "false", Importance.HIGH, "Use secure jetty server?")
        .define(JETTY_KEYSTORE_PATH, Type.STRING, Importance.HIGH, "Path the the keystore used by secure Jetty")
        .define(JETTY_KEYSTORE_PASSWORD, Type.STRING, Importance.HIGH, "keystore password")
        .define(JETTY_KEYSTORE_MANAGER_PASSWORD, Type.STRING, Importance.HIGH, "keystore manager password")
        .define(JETTY_TRUSTSTORE_PATH, Type.STRING, Importance.HIGH, "Path the the truststore used by secure Jetty")
        .define(JETTY_TRUSTSTORE_PASSWORD, Type.STRING, Importance.HIGH, "truststore password")
	    .define(JETTY_CLIENT_AUTH, Type.BOOLEAN, "true", Importance.HIGH, "require client authentication");

	public MarkLogicSourceConfig(final Map<?, ?> originals) {
		super(CONFIG_DEF, originals, false);
	}

}
