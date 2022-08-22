package com.marklogic.kafka.connect;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.SecurityContextType;
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.client.ext.modulesloader.ssl.SimpleX509TrustManager;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import org.apache.kafka.common.config.types.Password;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public class DefaultDatabaseClientConfigBuilder extends LoggingObject implements DatabaseClientConfigBuilder {

    @Override
    public DatabaseClientConfig buildDatabaseClientConfig(Map<String, Object> parsedConfig) {
        DatabaseClientConfig clientConfig = new DatabaseClientConfig();
        clientConfig.setCertFile((String) parsedConfig.get(MarkLogicSinkConfig.CONNECTION_CERT_FILE));
        Password certPassword = (Password) parsedConfig.get(MarkLogicSinkConfig.CONNECTION_CERT_PASSWORD);
        if (certPassword != null) {
            clientConfig.setCertPassword(certPassword.value());
        }
        clientConfig.setTrustManager(new SimpleX509TrustManager());
        configureHostNameVerifier(clientConfig, parsedConfig);
        String securityContextType = ((String) parsedConfig.get(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE)).toUpperCase();
        clientConfig.setSecurityContextType(SecurityContextType.valueOf(securityContextType));
        String database = (String) parsedConfig.get(MarkLogicSinkConfig.CONNECTION_DATABASE);
        if (database != null && database.trim().length() > 0) {
            clientConfig.setDatabase(database);
        }
        String connType = (String) parsedConfig.get(MarkLogicSinkConfig.CONNECTION_TYPE);
        if (connType != null && connType.trim().length() > 0) {
            clientConfig.setConnectionType(DatabaseClient.ConnectionType.valueOf(connType.toUpperCase()));
        }
        clientConfig.setExternalName((String) parsedConfig.get(MarkLogicSinkConfig.CONNECTION_EXTERNAL_NAME));
        clientConfig.setHost((String) parsedConfig.get(MarkLogicSinkConfig.CONNECTION_HOST));
        Password password = (Password) parsedConfig.get(MarkLogicSinkConfig.CONNECTION_PASSWORD);
        if (password != null) {
            clientConfig.setPassword(password.value());
        }
        clientConfig.setPort((Integer) parsedConfig.get(MarkLogicSinkConfig.CONNECTION_PORT));
        Boolean customSsl = (Boolean) parsedConfig.get(MarkLogicSinkConfig.ENABLE_CUSTOM_SSL);
        if (customSsl != null && customSsl) {
            configureCustomSslConnection(clientConfig, parsedConfig);
        }
        Boolean simpleSsl = (Boolean) parsedConfig.get(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL);
        if (simpleSsl != null && simpleSsl) {
            configureSimpleSsl(clientConfig);
        }
        clientConfig.setUsername((String) parsedConfig.get(MarkLogicSinkConfig.CONNECTION_USERNAME));
        return clientConfig;
    }

    /**
     * This provides a "simple" SSL configuration in that it uses the JVM's default SSLContext and
     * a "trust everything" hostname verifier. No default TrustManager is configured because in the absence of one,
     * the JVM's cacerts file will be used.
     *
     * @param clientConfig
     */
    private void configureSimpleSsl(DatabaseClientConfig clientConfig) {
        clientConfig.setSslContext(SimpleX509TrustManager.newSSLContext());
        clientConfig.setTrustManager(new SimpleX509TrustManager());
        clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
    }

    /**
     * This function configures the Host Name verifier based on the configuration.
     * ANY, STRICT and COMMON are the possible values, ANY being default.
     *
     * @param clientConfig
     */
    private void configureHostNameVerifier(DatabaseClientConfig clientConfig, Map<String, Object> parsedConfig) {
        String sslHostNameVerifier = (String) parsedConfig.get(MarkLogicSinkConfig.SSL_HOST_VERIFIER);
        if ("COMMON".equals(sslHostNameVerifier))
            clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.COMMON);
        else if ("STRICT".equals(sslHostNameVerifier))
            clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.STRICT);
        else
            clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
    }

    private void configureCustomSslConnection(DatabaseClientConfig clientConfig, Map<String, Object> parsedConfig) {
        String tlsVersion = (String) parsedConfig.get(MarkLogicSinkConfig.TLS_VERSION);
        Boolean sslMutualAuth = (Boolean) parsedConfig.get(MarkLogicSinkConfig.SSL_MUTUAL_AUTH);
        SSLContext sslContext = null;
        SecurityContextType securityContextType = clientConfig.getSecurityContextType();

        if (SecurityContextType.BASIC.equals(securityContextType) || SecurityContextType.DIGEST.equals(securityContextType)) {
            if (sslMutualAuth != null && sslMutualAuth) {
                /*2 way ssl changes*/
                KeyStore clientKeyStore = null;
                try {
                    clientKeyStore = KeyStore.getInstance("PKCS12");
                } catch (KeyStoreException e) {

                    throw new RuntimeException("Unable to get default SSLContext: " + e.getMessage(), e);
                }
                TrustManager[] trust = new TrustManager[]{new SimpleX509TrustManager()};

                try (InputStream keystoreInputStream = new FileInputStream(clientConfig.getCertFile())) {
                    clientKeyStore.load(keystoreInputStream, clientConfig.getCertPassword().toCharArray());
                } catch (Exception e) {
                    throw new RuntimeException("Unable to configure custom SSL connection: " + e.getMessage(), e);
                }
                KeyManagerFactory keyManagerFactory = null;
                try {
                    keyManagerFactory = KeyManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                } catch (Exception e) {

                    throw new RuntimeException("Unable to configure custom SSL connection: " + e.getMessage(), e);
                }
                try {
                    keyManagerFactory.init(clientKeyStore, clientConfig.getCertPassword().toCharArray());
                } catch (Exception e) {

                    throw new RuntimeException("Unable to configure custom SSL connection: " + e.getMessage(), e);
                }
                KeyManager[] key = keyManagerFactory.getKeyManagers();
                try {
                    sslContext = SSLContext.getInstance(tlsVersion);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("Unable to configure custom SSL connection:" + e.getMessage(), e);
                }
                try {
                    sslContext.init(key, trust, null);
                } catch (KeyManagementException e) {
                    throw new RuntimeException("Unable to configure custom SSL connection:" + e.getMessage(), e);
                }
                clientConfig.setSslContext(sslContext);
            } else {/* 1-way ssl */
                TrustManager[] trust = new TrustManager[]{new SimpleX509TrustManager()};
                try {
                    sslContext = SSLContext.getInstance(tlsVersion);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("Unable to configure custom SSL connection: " + e.getMessage(), e);
                }
                try {
                    sslContext.init(null, trust, null);
                } catch (KeyManagementException e) {
                    throw new RuntimeException("Unable to configure custom SSL connection:" + e.getMessage(), e);
                }
                clientConfig.setSslContext(sslContext);
            }
        }
    }
}
