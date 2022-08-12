package com.marklogic.kafka.connect;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.SecurityContextType;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BuildDatabaseClientConfigTest {

    DefaultDatabaseClientConfigBuilder builder = new DefaultDatabaseClientConfigBuilder();
    Map<String, Object> config = new HashMap<>();

    @BeforeEach
    public void setup() {
        config.put(MarkLogicSinkConfig.CONNECTION_HOST, "some-host");
        config.put(MarkLogicSinkConfig.CONNECTION_PORT, 8123);
        config.put(MarkLogicSinkConfig.CONNECTION_DATABASE, "some-database");
    }

    @Test
    public void basicAuthentication() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "basic");
        config.put(MarkLogicSinkConfig.CONNECTION_USERNAME, "some-user");
        config.put(MarkLogicSinkConfig.CONNECTION_PASSWORD, "some-password");

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals("some-host", clientConfig.getHost());
        assertEquals(8123, clientConfig.getPort());
        assertEquals("some-database", clientConfig.getDatabase());
        assertEquals(SecurityContextType.BASIC, clientConfig.getSecurityContextType());
        assertEquals("some-user", clientConfig.getUsername());
        assertEquals("some-password", clientConfig.getPassword());
    }

    @Test
    public void certificateAuthentication() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "certificate");
        config.put(MarkLogicSinkConfig.CONNECTION_CERT_FILE, "/path/to/file");
        config.put(MarkLogicSinkConfig.CONNECTION_CERT_PASSWORD, "cert-password");

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.CERTIFICATE, clientConfig.getSecurityContextType());
        assertEquals("/path/to/file", clientConfig.getCertFile());
        assertEquals("cert-password", clientConfig.getCertPassword());
    }

    @Test
    public void kerberosAuthentication() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "kerberos");
        config.put(MarkLogicSinkConfig.CONNECTION_EXTERNAL_NAME, "some-name");

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.KERBEROS, clientConfig.getSecurityContextType());
        assertEquals("some-name", clientConfig.getExternalName());
    }

    @Test
    public void digestAuthenticationAndSimpleSsl() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "digest");
        config.put(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL, true);

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.DIGEST, clientConfig.getSecurityContextType());
        assertNotNull(clientConfig.getSslContext());
        assertNotNull(clientConfig.getSslHostnameVerifier());
        assertNotNull(clientConfig.getTrustManager());
    }

    @Test
    public void basicAuthenticationAndSimpleSsl() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "basic");
        config.put(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL, true);

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.BASIC, clientConfig.getSecurityContextType());
        assertNotNull(clientConfig.getSslContext());
        assertNotNull(clientConfig.getSslHostnameVerifier());
        assertNotNull(clientConfig.getTrustManager());
    }

    @Test
    public void basicAuthenticationAndMutualSSL() {
        File file = new File("src/test/resources/srportal.p12");
        String absolutePath = file.getAbsolutePath();
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "basic");
        config.put(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL, false);
        config.put(MarkLogicSinkConfig.SSL, true);
        config.put(MarkLogicSinkConfig.TLS_VERSION, "TLSv1.2");
        config.put(MarkLogicSinkConfig.SSL_HOST_VERIFIER, "STRICT");
        config.put(MarkLogicSinkConfig.SSL_MUTUAL_AUTH, "true");
        config.put(MarkLogicSinkConfig.CONNECTION_CERT_FILE, absolutePath);
        config.put(MarkLogicSinkConfig.CONNECTION_CERT_PASSWORD, "abc");

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.BASIC, clientConfig.getSecurityContextType());
        assertNotNull(clientConfig.getSslContext());
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.STRICT, clientConfig.getSslHostnameVerifier());
        assertNotNull(clientConfig.getTrustManager());
    }

    @Test
    public void basicAuthenticationAndMutualSSLWithInvalidHost() {
        File file = new File("src/test/resources/srportal.p12");
        String absolutePath = file.getAbsolutePath();
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "basic");
        config.put(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL, false);
        config.put(MarkLogicSinkConfig.SSL, true);
        config.put(MarkLogicSinkConfig.TLS_VERSION, "TLSv1.2");
        config.put(MarkLogicSinkConfig.SSL_HOST_VERIFIER, "SOMETHING");
        config.put(MarkLogicSinkConfig.SSL_MUTUAL_AUTH, "true");
        config.put(MarkLogicSinkConfig.CONNECTION_CERT_FILE, absolutePath);
        config.put(MarkLogicSinkConfig.CONNECTION_CERT_PASSWORD, "abc");

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.BASIC, clientConfig.getSecurityContextType());
        assertNotNull(clientConfig.getSslContext());
        assertEquals(DatabaseClientFactory.SSLHostnameVerifier.ANY, clientConfig.getSslHostnameVerifier());
        System.out.println(clientConfig.getSslHostnameVerifier());
        assertNotNull(clientConfig.getTrustManager());
    }


    @Test
    public void digestAuthenticationAnd1WaySSL() {

        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "digest");
        config.put(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL, false);
        config.put(MarkLogicSinkConfig.SSL, true);
        config.put(MarkLogicSinkConfig.TLS_VERSION, "TLSv1.2");
        config.put(MarkLogicSinkConfig.SSL_HOST_VERIFIER, "STRICT");
        config.put(MarkLogicSinkConfig.SSL_MUTUAL_AUTH, "false");

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(SecurityContextType.DIGEST, clientConfig.getSecurityContextType());
        assertNotNull(clientConfig.getSslContext());
        assertNotNull(clientConfig.getSslHostnameVerifier());
        assertNotNull(clientConfig.getTrustManager());
    }

    @Test
    public void gatewayConnection() {
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "digest");
        config.put(MarkLogicSinkConfig.CONNECTION_TYPE, "gateway");

        DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
        assertEquals(DatabaseClient.ConnectionType.GATEWAY, clientConfig.getConnectionType());
    }

    @Test
    // This also implicitly verifies that all other sink properties are optional
    public void testMissingRequired() {
        Map<String, Object> allRequiredValuesConfig = new HashMap<>();
        allRequiredValuesConfig.put(MarkLogicSinkConfig.CONNECTION_HOST, "");
        allRequiredValuesConfig.put(MarkLogicSinkConfig.CONNECTION_PORT, 8000);
        allRequiredValuesConfig.put(MarkLogicSinkConfig.CONNECTION_USERNAME, "");
        allRequiredValuesConfig.put(MarkLogicSinkConfig.CONNECTION_PASSWORD, "");
        MarkLogicSinkConfig.CONFIG_DEF.parse(allRequiredValuesConfig);

        Set<String> keys = allRequiredValuesConfig.keySet();
        for (String key : keys) {
            Assertions.assertThrows(ConfigException.class, () -> {
                HashMap<String, Object> missingSingleValueConfig = new HashMap<>(allRequiredValuesConfig);
                missingSingleValueConfig.remove(key);
                MarkLogicSinkConfig.CONFIG_DEF.parse(missingSingleValueConfig);
            });
        }
    }
}
