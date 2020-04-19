package com.marklogic.kafka.connect;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.SecurityContextType;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public class DefaultDatabaseClientConfigBuilder implements DatabaseClientConfigBuilder {

	@Override
	public DatabaseClientConfig buildDatabaseClientConfig(Map<String, String> kafkaConfig) {
		DatabaseClientConfig clientConfig = new DatabaseClientConfig();
		clientConfig.setCertFile(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_CERT_FILE));
		clientConfig.setCertPassword(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_CERT_PASSWORD));

		String type = kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_TYPE);
		if (type != null && type.trim().length() > 0) {
			clientConfig.setConnectionType(DatabaseClient.ConnectionType.valueOf(type.toUpperCase()));
		}

		String database = kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_DATABASE);
		if (database != null && database.trim().length() > 0) {
			clientConfig.setDatabase(database);
		}

		clientConfig.setExternalName(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_EXTERNAL_NAME));
		clientConfig.setHost(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_HOST));
		clientConfig.setPassword(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_PASSWORD));
		clientConfig.setPort(Integer.parseInt(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_PORT)));

		String securityContextType = kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE).toUpperCase();
		clientConfig.setSecurityContextType(SecurityContextType.valueOf(securityContextType));

		String simpleSsl = kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL);
		if (simpleSsl != null && Boolean.parseBoolean(simpleSsl)) {
			configureSimpleSsl(clientConfig);
		}

		clientConfig.setUsername(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_USERNAME));

		return clientConfig;
	}

	/**
	 * This provides a "simple" SSL configuration in that it uses the JVM's default SSLContext and
	 * a "trust everything" hostname verifier. No default TrustManager is configured because in the absence of one,
	 * the JVM's cacerts file will be used.
	 *
	 * @param clientConfig
	 */
	protected void configureSimpleSsl(DatabaseClientConfig clientConfig) {
		try {
			clientConfig.setSslContext(SSLContext.getDefault());
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Unable to get default SSLContext: " + e.getMessage(), e);
		}

		clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
	}
}
