package com.marklogic.kafka.connect.source.jetty;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MarkLogicSourceConnector extends SourceConnector {

    private static Logger logger = LoggerFactory.getLogger(MarkLogicSourceConnector.class);

    private Server server;
    private int port;
    private Map<String, String> config;

    public static final String MARKLOGIC_CONNECTOR_VERSION = "0.9.0";

    @Override
    public void start(Map<String, String> props) {
        logger.info("in MarkLogicSourceConnector start()");
        config = props;
        port = Integer.parseInt(config.get(MarkLogicSourceConfig.JETTY_PORT));
        boolean isSecureServer = Boolean.parseBoolean(config.get(MarkLogicSourceConfig.JETTY_SECURE));

        if (isSecureServer) {
            logger.info("starting Secure Jetty server on port {}", port);
            String keystorePath = config.get(MarkLogicSourceConfig.JETTY_KEYSTORE_PATH);
            String keystorePassword = config.get(MarkLogicSourceConfig.JETTY_KEYSTORE_PASSWORD);
            String keystoreManagerPassword = config.get(MarkLogicSourceConfig.JETTY_KEYSTORE_MANAGER_PASSWORD);
            String truststorePath = config.get(MarkLogicSourceConfig.JETTY_TRUSTSTORE_PATH);
            String truststorePassword = config.get(MarkLogicSourceConfig.JETTY_TRUSTSTORE_PASSWORD);
            boolean clientAuth = Boolean.parseBoolean(config.get(MarkLogicSourceConfig.JETTY_CLIENT_AUTH));
            logger.info("  keystore: {}", keystorePath);
            logger.info("  truststore: {}", truststorePath);
            server = ProducerServerFactory.createSecureServer(port,
                    keystorePath,
                    keystorePassword,
                    keystoreManagerPassword,
                    truststorePath,
                    truststorePassword,
                    clientAuth);
        } else {
            logger.info("starting *unsecured* Jetty server on port {}", port);
            server = ProducerServerFactory.createServer(port);
        }

        try {
            server.start();
            //server.join();
        } catch (Exception ex) {
            logger.error("Error starting Jetty", ex);
            context.raiseError(ex);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MarkLogicJettySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("in MarkLogicSourceConnector taskConfigs()");
        List<Map<String, String>> configs = new ArrayList<>();
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        logger.info("in MarkLogicSourceConnector stop()");

        try {
            logger.info("stopping Jetty server");
            server.stop();
        } catch (Exception ex) {
            logger.error("Error stopping Jetty", ex);
            context.raiseError(ex);
        }
    }

    @Override
    public ConfigDef config() {
        return MarkLogicSourceConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return MARKLOGIC_CONNECTOR_VERSION;
    }
}
