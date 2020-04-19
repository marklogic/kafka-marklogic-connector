package com.marklogic.kafka.connect;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.SecurityContextType;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class BuildDatabaseClientConfigTest {

	DefaultDatabaseClientConfigBuilder builder = new DefaultDatabaseClientConfigBuilder();
	Map<String, String> config = new HashMap<>();

	@BeforeEach
	public void setup() {
		config.put(MarkLogicSinkConfig.CONNECTION_HOST, "some-host");
		config.put(MarkLogicSinkConfig.CONNECTION_PORT, "8123");
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
		config.put(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL, "true");

		DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
		assertEquals(SecurityContextType.DIGEST, clientConfig.getSecurityContextType());
		assertNotNull(clientConfig.getSslContext());
		assertNotNull(clientConfig.getSslHostnameVerifier());
		assertNull(clientConfig.getTrustManager(), "If DatabaseClientFactory is given a null TrustManager, it will " +
			"default to the JVM's cacerts file, which is a reasonable default approach");
	}

	@Test
	public void gatewayConnection() {
		config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "digest");
		config.put(MarkLogicSinkConfig.CONNECTION_TYPE, "gateway");

		DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
		assertEquals(DatabaseClient.ConnectionType.GATEWAY, clientConfig.getConnectionType());
	}
}
