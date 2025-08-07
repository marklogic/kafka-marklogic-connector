/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
package com.marklogic.kafka.connect;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.SecurityContextType;
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.client.ext.modulesloader.ssl.SimpleX509TrustManager;
import org.apache.kafka.common.config.types.Password;
import org.springframework.util.StringUtils;

import javax.net.ssl.*;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.util.Map;

public class DefaultDatabaseClientConfigBuilder extends LoggingObject implements DatabaseClientConfigBuilder {

    @Override
    public DatabaseClientConfig buildDatabaseClientConfig(Map<String, Object> parsedConfig) {
        DatabaseClientConfig clientConfig = new DatabaseClientConfig();

        clientConfig.setHost((String) parsedConfig.get(MarkLogicConfig.CONNECTION_HOST));
        clientConfig.setPort((Integer) parsedConfig.get(MarkLogicConfig.CONNECTION_PORT));
        clientConfig.setBasePath((String) parsedConfig.get(MarkLogicConfig.CONNECTION_BASE_PATH));

        String securityContextType = ((String) parsedConfig.get(MarkLogicConfig.CONNECTION_SECURITY_CONTEXT_TYPE))
                .toUpperCase();
        clientConfig.setSecurityContextType(SecurityContextType.valueOf(securityContextType));

        String database = (String) parsedConfig.get(MarkLogicConfig.CONNECTION_DATABASE);
        if (StringUtils.hasText(database)) {
            clientConfig.setDatabase(database);
        }
        String connType = (String) parsedConfig.get(MarkLogicConfig.CONNECTION_TYPE);
        if (StringUtils.hasText(connType)) {
            clientConfig.setConnectionType(DatabaseClient.ConnectionType.valueOf(connType.toUpperCase()));
        }

        clientConfig.setUsername((String) parsedConfig.get(MarkLogicConfig.CONNECTION_USERNAME));
        Password password = (Password) parsedConfig.get(MarkLogicConfig.CONNECTION_PASSWORD);
        if (password != null) {
            clientConfig.setPassword(password.value());
        }

        clientConfig.setCertFile((String) parsedConfig.get(MarkLogicConfig.CONNECTION_CERT_FILE));
        Password certPassword = (Password) parsedConfig.get(MarkLogicConfig.CONNECTION_CERT_PASSWORD);
        if (certPassword != null) {
            clientConfig.setCertPassword(certPassword.value());
        }

        clientConfig.setExternalName((String) parsedConfig.get(MarkLogicConfig.CONNECTION_EXTERNAL_NAME));

        if (ConfigUtil.getBoolean(MarkLogicConfig.ENABLE_CUSTOM_SSL, parsedConfig)) {
            clientConfig.setTrustManager(new SimpleX509TrustManager());
            configureCustomSslContext(clientConfig, parsedConfig);
            configureHostNameVerifier(clientConfig, parsedConfig);
        }
        if (ConfigUtil.getBoolean(MarkLogicConfig.CONNECTION_SIMPLE_SSL, parsedConfig)) {
            configureSimpleSsl(clientConfig);
        }

        clientConfig.setCloudApiKey((String) parsedConfig.get(MarkLogicConfig.CONNECTION_CLOUD_API_KEY));

        return clientConfig;
    }

    /**
     * This provides a "simple" SSL configuration in that it uses the JVM's default
     * SSLContext and
     * a "trust everything" hostname verifier. No default TrustManager is configured
     * because in the absence of one,
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
        String sslHostNameVerifier = (String) parsedConfig.get(MarkLogicConfig.SSL_HOST_VERIFIER);
        if ("COMMON".equals(sslHostNameVerifier))
            clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.COMMON);
        else if ("STRICT".equals(sslHostNameVerifier))
            clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.STRICT);
        else
            clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
    }

    private void configureCustomSslContext(DatabaseClientConfig clientConfig, Map<String, Object> parsedConfig) {
        final SecurityContextType securityContextType = clientConfig.getSecurityContextType();
        if (SecurityContextType.BASIC.equals(securityContextType)
                || SecurityContextType.DIGEST.equals(securityContextType)) {
            SSLContext sslContext = ConfigUtil.getBoolean(MarkLogicConfig.SSL_MUTUAL_AUTH, parsedConfig)
                    ? buildTwoWaySslContext(clientConfig.getCertFile(), clientConfig.getCertPassword(), parsedConfig)
                    : buildOneWaySslContext(parsedConfig);
            clientConfig.setSslContext(sslContext);
        }
    }

    private SSLContext buildTwoWaySslContext(String certFile, String certPassword, Map<String, Object> parsedConfig) {
        final String tlsVersion = (String) parsedConfig.get(MarkLogicConfig.TLS_VERSION);

        KeyStore clientKeyStore;
        try {
            clientKeyStore = KeyStore.getInstance("PKCS12");
        } catch (KeyStoreException e) {
            throw new MarkLogicConnectorException("Unable to get default SSLContext: " + e.getMessage(), e);
        }

        KeyManagerFactory keyManagerFactory;
        SSLContext sslContext;
        try (InputStream keystoreInputStream = Files.newInputStream(Paths.get(certFile))) {
            clientKeyStore.load(keystoreInputStream, certPassword.toCharArray());
            keyManagerFactory = KeyManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(clientKeyStore, certPassword.toCharArray());
            sslContext = SSLContext.getInstance(tlsVersion);

            TrustManager[] trust = new TrustManager[] { new SimpleX509TrustManager() };
            KeyManager[] key = keyManagerFactory.getKeyManagers();
            sslContext.init(key, trust, null);
            return sslContext;
        } catch (Exception e) {
            throw new MarkLogicConnectorException("Unable to configure custom SSL connection: " + e.getMessage(), e);
        }
    }

    private SSLContext buildOneWaySslContext(Map<String, Object> parsedConfig) {
        final String tlsVersion = (String) parsedConfig.get(MarkLogicConfig.TLS_VERSION);
        TrustManager[] trust = new TrustManager[] { new SimpleX509TrustManager() };
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance(tlsVersion);
            sslContext.init(null, trust, null);
            return sslContext;
        } catch (Exception e) {
            throw new MarkLogicConnectorException("Unable to configure custom SSL connection:" + e.getMessage(), e);
        }
    }
}
