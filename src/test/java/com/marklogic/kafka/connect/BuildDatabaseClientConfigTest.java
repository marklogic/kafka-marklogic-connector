/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.SecurityContextType;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BuildDatabaseClientConfigTest {

    DefaultDatabaseClientConfigBuilder builder = new DefaultDatabaseClientConfigBuilder();
    Map<String, Object> config = new HashMap<>();

    @BeforeEach
    void setup() {
        config.put(MarkLogicSinkConfig.CONNECTION_HOST, "some-host");
        config.put(MarkLogicSinkConfig.CONNECTION_PORT, 8123);
        config.put(MarkLogicSinkConfig.CONNECTION_DATABASE, "some-database");
    }

    @Test
    void basicAuthentication() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "basic");
        config.put(MarkLogicSinkConfig.CONNECTION_USERNAME, "some-user");
        config.put(MarkLogicSinkConfig.CONNECTION_PASSWORD, new Password("some-password"));

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals("some-host", clientConfig.getHost());
        assertEquals(8123, clientConfig.getPort());
        assertEquals("some-database", clientConfig.getDatabase());
        assertEquals(SecurityContextType.BASIC, clientConfig.getSecurityContextType());
        assertEquals("some-user", clientConfig.getUsername());
        assertEquals("some-password", clientConfig.getPassword());
    }

    @Test
    void certificateAuthentication() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "certificate");
        config.put(MarkLogicSinkConfig.CONNECTION_CERT_FILE, "/path/to/file");
        config.put(MarkLogicSinkConfig.CONNECTION_CERT_PASSWORD, new Password("cert-password"));

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.CERTIFICATE, clientConfig.getSecurityContextType());
        assertEquals("/path/to/file", clientConfig.getCertFile());
        assertEquals("cert-password", clientConfig.getCertPassword());
    }

    @Test
    void kerberosAuthentication() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "kerberos");
        config.put(MarkLogicSinkConfig.CONNECTION_EXTERNAL_NAME, "some-name");

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.KERBEROS, clientConfig.getSecurityContextType());
        assertEquals("some-name", clientConfig.getExternalName());
    }

    @Test
    void cloudAuthentication() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "cloud");
        config.put(MarkLogicSinkConfig.CONNECTION_CLOUD_API_KEY, "my-key");
        config.put(MarkLogicSinkConfig.CONNECTION_BASE_PATH, "/my/path");

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.CLOUD, clientConfig.getSecurityContextType());
        assertEquals("my-key", clientConfig.getCloudApiKey());
        assertEquals("/my/path", clientConfig.getBasePath());
    }

    @Test
    void digestAuthenticationAndSimpleSsl() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "digest");
        config.put(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL, true);

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.DIGEST, clientConfig.getSecurityContextType());
        assertNotNull(clientConfig.getSslContext());
        assertNotNull(clientConfig.getSslHostnameVerifier());
        assertNotNull(clientConfig.getTrustManager());
    }

    @Test
    void basicAuthenticationAndSimpleSsl() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "basic");
        config.put(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL, true);

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.BASIC, clientConfig.getSecurityContextType());
        assertNotNull(clientConfig.getSslContext());
        assertNotNull(clientConfig.getSslHostnameVerifier());
        assertNotNull(clientConfig.getTrustManager());
    }

    @Test
    void basicAuthenticationAndMutualSSL() {
        File file = new File("src/test/resources/srportal.p12");
        String absolutePath = file.getAbsolutePath();
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "basic");
        config.put(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL, false);
        config.put(MarkLogicSinkConfig.ENABLE_CUSTOM_SSL, true);
        config.put(MarkLogicSinkConfig.TLS_VERSION, "TLSv1.2");
        config.put(MarkLogicSinkConfig.SSL_HOST_VERIFIER, "STRICT");
        config.put(MarkLogicSinkConfig.SSL_MUTUAL_AUTH, true);
        config.put(MarkLogicSinkConfig.CONNECTION_CERT_FILE, absolutePath);
        config.put(MarkLogicSinkConfig.CONNECTION_CERT_PASSWORD, new Password("abc"));

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.BASIC, clientConfig.getSecurityContextType());
        assertNotNull(clientConfig.getSslContext());
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.STRICT, clientConfig.getSslHostnameVerifier());
        assertNotNull(clientConfig.getTrustManager());
    }

    @Test
    void basicAuthenticationAndMutualSSLWithInvalidHost() {
        File file = new File("src/test/resources/srportal.p12");
        String absolutePath = file.getAbsolutePath();
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "basic");
        config.put(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL, false);
        config.put(MarkLogicSinkConfig.ENABLE_CUSTOM_SSL, true);
        config.put(MarkLogicSinkConfig.TLS_VERSION, "TLSv1.2");
        config.put(MarkLogicSinkConfig.SSL_HOST_VERIFIER, "SOMETHING");
        config.put(MarkLogicSinkConfig.SSL_MUTUAL_AUTH, true);
        config.put(MarkLogicSinkConfig.CONNECTION_CERT_FILE, absolutePath);
        config.put(MarkLogicSinkConfig.CONNECTION_CERT_PASSWORD, new Password("abc"));

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.BASIC, clientConfig.getSecurityContextType());
        assertNotNull(clientConfig.getSslContext());
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.ANY, clientConfig.getSslHostnameVerifier());
        assertNotNull(clientConfig.getTrustManager());
    }


    @Test
    void digestAuthenticationAnd1WaySSL() {

        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "digest");
        config.put(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL, false);
        config.put(MarkLogicSinkConfig.ENABLE_CUSTOM_SSL, true);
        config.put(MarkLogicSinkConfig.TLS_VERSION, "TLSv1.2");
        config.put(MarkLogicSinkConfig.SSL_HOST_VERIFIER, "STRICT");
        config.put(MarkLogicSinkConfig.SSL_MUTUAL_AUTH, false);

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.DIGEST, clientConfig.getSecurityContextType());
        assertNotNull(clientConfig.getSslContext());
        assertNotNull(clientConfig.getSslHostnameVerifier());
        assertNotNull(clientConfig.getTrustManager());
    }

    @Test
    void gatewayConnection() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "digest");
        config.put(MarkLogicSinkConfig.CONNECTION_TYPE, "gateway");

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(DatabaseClient.ConnectionType.GATEWAY, clientConfig.getConnectionType());
    }

    @Test
    void testInvalidAuthentication() {
        Map<String, Object> securityContextConfig = new HashMap<>();
        securityContextConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        securityContextConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        securityContextConfig.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "IncorrectValue");
        ConfigException ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(securityContextConfig),
            "Should throw ConfigException when an invalid authentication type is provided.");
        assertEquals("Invalid value: IncorrectValue; must be one of: [DIGEST, BASIC, CERTIFICATE, KERBEROS, NONE]", ex.getMessage());
    }

    @Test
    void invalidDocumentFormatConfig() {
        Map<String, Object>  documentFormatConfig = new HashMap<>();
        documentFormatConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        documentFormatConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        documentFormatConfig.put(MarkLogicSinkConfig.DOCUMENT_FORMAT, "InvalidFormat");
        ConfigException ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(documentFormatConfig),
            "Should throw ConfigException when an invalid Document format is provided.");
        assertEquals("Invalid value: InvalidFormat; must be one of: [JSON, XML, BINARY, TEXT, UNKNOWN, ]", ex.getMessage());
    }

    @Test
    void invalidIdStrategyConfig() {
        Map<String, Object> idStrategyConfig = new HashMap<>();
        idStrategyConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        idStrategyConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        idStrategyConfig.put(MarkLogicSinkConfig.ID_STRATEGY, "InvalidStrategy");
        ConfigException ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(idStrategyConfig),
            "Should throw ConfigException when an Id Strategy format is provided.");
        assertEquals("Invalid value: InvalidStrategy; must be one of: [JSONPATH, HASH, KAFKA_META_HASHED, KAFKA_META_WITH_SLASH, ]", ex.getMessage());
    }

    @Test
    void invalidHostConfig() {
        Map<String, Object> invalidHostConfig = new HashMap<>();
        invalidHostConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "");
        invalidHostConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        ConfigException ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(invalidHostConfig),
            "Should throw ConfigException when host is empty.");
        assertEquals("Invalid value  for configuration ml.connection.host: String must be non-empty", ex.getMessage());
    }

    @Test
    void invalidPortConfig() {
        Map<String, Object> invalidPortConfig = new HashMap<>();
        invalidPortConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        invalidPortConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, null);
        ConfigException ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(invalidPortConfig),
            "Should throw ConfigException when port is empty.");
        assertEquals("Invalid value null for configuration ml.connection.port: Value must be non-null", ex.getMessage());
    }

    @Test
    void invalidSSLHostVerifierConfig() {
        Map<String, Object> sslHostVerifierConfig = new HashMap<>();
        sslHostVerifierConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        sslHostVerifierConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        sslHostVerifierConfig.put(MarkLogicSinkConfig.SSL_HOST_VERIFIER, "InvalidValue");
        ConfigException ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(sslHostVerifierConfig),
            "Should throw ConfigException when port is empty.");
        assertEquals("Invalid value: InvalidValue; must be one of: [ANY, COMMON, STRICT]", ex.getMessage());
    }

    @Test
    void invalidConnectTypeConfig() {
        Map<String, Object> connectTypeConfig = new HashMap<>();
        connectTypeConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        connectTypeConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        connectTypeConfig.put(MarkLogicSinkConfig.CONNECTION_TYPE, "InvalidValue");
        ConfigException ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(connectTypeConfig),
            "Should throw ConfigException when port is empty.");
        assertEquals("Invalid value: InvalidValue; must be one of: [DIRECT, GATEWAY, ]", ex.getMessage());
    }

    @Test
    void invalidDmsdkConfig() {
        ConfigException ex = null;

        Map<String, Object> dmsdkNegBatchSizeConfig = new HashMap<>();
        dmsdkNegBatchSizeConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        dmsdkNegBatchSizeConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        dmsdkNegBatchSizeConfig.put(MarkLogicSinkConfig.DMSDK_BATCH_SIZE, -1);
        ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(dmsdkNegBatchSizeConfig),
            "Should throw ConfigException when an invalid Document format is provided.");
        assertEquals("Invalid value -1 for configuration ml.dmsdk.batchSize: Value must be at least 1", ex.getMessage());

        Map<String, Object> dmsdkZeroBatchSizeConfig = new HashMap<>();
        dmsdkZeroBatchSizeConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        dmsdkZeroBatchSizeConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        dmsdkZeroBatchSizeConfig.put(MarkLogicSinkConfig.DMSDK_BATCH_SIZE, 0);
        ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(dmsdkZeroBatchSizeConfig),
            "Should throw ConfigException when an invalid Document format is provided.");
        assertEquals("Invalid value 0 for configuration ml.dmsdk.batchSize: Value must be at least 1", ex.getMessage());

        Map<String, Object> dmsdkNegThreadCountConfig = new HashMap<>();
        dmsdkNegThreadCountConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        dmsdkNegThreadCountConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        dmsdkNegThreadCountConfig.put(MarkLogicSinkConfig.DMSDK_THREAD_COUNT, -1);
        ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(dmsdkNegThreadCountConfig),
            "Should throw ConfigException when an invalid Document format is provided.");
        assertEquals("Invalid value -1 for configuration ml.dmsdk.threadCount: Value must be at least 1", ex.getMessage());

        Map<String, Object> dmsdkZeroThreadCountConfig = new HashMap<>();
        dmsdkZeroThreadCountConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        dmsdkZeroThreadCountConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        dmsdkZeroThreadCountConfig.put(MarkLogicSinkConfig.DMSDK_THREAD_COUNT, 0);
        ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(dmsdkZeroThreadCountConfig),
            "Should throw ConfigException when an invalid Document format is provided.");
        assertEquals("Invalid value 0 for configuration ml.dmsdk.threadCount: Value must be at least 1", ex.getMessage());
    }

    @Test
    void invalidBulkDSConfig() {
        ConfigException ex = null;

        Map<String, Object> negBulkDsBatchSize = new HashMap<>();
        negBulkDsBatchSize.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        negBulkDsBatchSize.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        negBulkDsBatchSize.put(MarkLogicSinkConfig.BULK_DS_BATCH_SIZE, -1);
        ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(negBulkDsBatchSize),
            "Should throw ConfigException when an invalid Document format is provided.");
        assertEquals("Invalid value -1 for configuration ml.sink.bulkds.batchSize: Value must be at least 1", ex.getMessage());

        Map<String, Object> zeroBulkDsBatchSize = new HashMap<>();
        zeroBulkDsBatchSize.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        zeroBulkDsBatchSize.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        zeroBulkDsBatchSize.put(MarkLogicSinkConfig.BULK_DS_BATCH_SIZE, 0);
        ex = assertThrows(ConfigException.class, () -> MarkLogicSinkConfig.CONFIG_DEF.parse(zeroBulkDsBatchSize),
            "Should throw ConfigException when an invalid Document format is provided.");
        assertEquals("Invalid value 0 for configuration ml.sink.bulkds.batchSize: Value must be at least 1", ex.getMessage());
    }

    @Test
    // This also implicitly verifies that all other sink properties are optional
    void testMissingRequired() {
        Map<String, Object> allRequiredValuesConfig = new HashMap<>();
        allRequiredValuesConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        allRequiredValuesConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        MarkLogicSinkConfig.CONFIG_DEF.parse(allRequiredValuesConfig);

        Set<String> keys = allRequiredValuesConfig.keySet();
        for (String key : keys) {
            HashMap<String, Object> missingSingleValueConfig = new HashMap<>(allRequiredValuesConfig);
            missingSingleValueConfig.remove(key);
            assertThrows(ConfigException.class, () -> {
                MarkLogicSinkConfig.CONFIG_DEF.parse(missingSingleValueConfig);
            });
        }
    }
}
