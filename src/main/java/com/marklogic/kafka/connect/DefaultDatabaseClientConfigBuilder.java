package com.marklogic.kafka.connect;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.SecurityContextType;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
<<<<<<< HEAD
=======

>>>>>>> upstream/master
import com.marklogic.client.ext.modulesloader.ssl.SimpleX509TrustManager;

public class DefaultDatabaseClientConfigBuilder implements DatabaseClientConfigBuilder {
	
	@Override
	public DatabaseClientConfig buildDatabaseClientConfig(Map<String, String> kafkaConfig) {
		DatabaseClientConfig clientConfig = new DatabaseClientConfig();
		clientConfig.setCertFile(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_CERT_FILE));
		clientConfig.setCertPassword(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_CERT_PASSWORD));
		clientConfig.setTrustManager(new SimpleX509TrustManager());
		clientConfig = configureHostNameVerifier(clientConfig,kafkaConfig);
<<<<<<< HEAD
		String securityContextType = kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE).toUpperCase();
		clientConfig.setSecurityContextType(SecurityContextType.valueOf(securityContextType));
=======
>>>>>>> upstream/master
		String database = kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_DATABASE);
		if (database != null && database.trim().length() > 0) {
			clientConfig.setDatabase(database);
		}
		String connType = kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_TYPE);
		if (connType != null && connType.trim().length() > 0) {
			clientConfig.setConnectionType(DatabaseClient.ConnectionType.valueOf(connType.toUpperCase()));
		}
		clientConfig.setExternalName(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_EXTERNAL_NAME));
		clientConfig.setHost(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_HOST));
		clientConfig.setPassword(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_PASSWORD));
		clientConfig.setPort(Integer.parseInt(kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_PORT)));
<<<<<<< HEAD
		String customSsl = kafkaConfig.get(MarkLogicSinkConfig.SSL);
		if (customSsl != null && Boolean.parseBoolean(customSsl)) {
			clientConfig = configureCustomSslConnection(clientConfig, kafkaConfig);
		}
=======
		clientConfig = configureCustomSslConnection(clientConfig, kafkaConfig);
>>>>>>> upstream/master
		String simpleSsl = kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_SIMPLE_SSL);
		if (simpleSsl != null && Boolean.parseBoolean(simpleSsl)) {
			clientConfig = configureSimpleSsl(clientConfig);
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
	protected DatabaseClientConfig configureSimpleSsl(DatabaseClientConfig clientConfig) {
<<<<<<< HEAD
		
		clientConfig.setSslContext(SimpleX509TrustManager.newSSLContext());
		clientConfig.setTrustManager(new SimpleX509TrustManager());
=======
		try {
			clientConfig.setSslContext(SSLContext.getDefault());
			clientConfig.setTrustManager(new SimpleX509TrustManager());
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Unable to get default SSLContext: " + e.getMessage(), e);
		}
>>>>>>> upstream/master
		clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
		return clientConfig;
	}
	
<<<<<<< HEAD
	
=======
>>>>>>> upstream/master
	/**
	 * This function configures the Host Name verifier based on the configuration. 
	 * ANY, STRICT and COMMON are the possible values, ANY being default. 
	 *
	 * @param clientConfig
	 */
	protected DatabaseClientConfig configureHostNameVerifier(DatabaseClientConfig clientConfig, Map<String, String> kafkaConfig) {
		String sslHostNameVerifier = kafkaConfig.get(MarkLogicSinkConfig.SSL_HOST_VERIFIER);
		if ("ANY".equals(sslHostNameVerifier))
			clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
		else if ("COMMON".equals(sslHostNameVerifier))
			clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.COMMON);
		else if ("STRICT".equals(sslHostNameVerifier))
			clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.STRICT);
		else 
			clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
		return clientConfig;
	}
	
	protected DatabaseClientConfig configureCustomSslConnection(DatabaseClientConfig clientConfig, Map<String, String> kafkaConfig) {
		String ssl = kafkaConfig.get(MarkLogicSinkConfig.SSL);
		String tlsVersion = kafkaConfig.get(MarkLogicSinkConfig.TLS_VERSION);
		String sslMutualAuth = kafkaConfig.get(MarkLogicSinkConfig.SSL_MUTUAL_AUTH);
		SSLContext sslContext = null;
		String securityContextType = kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE).toUpperCase();
		clientConfig.setSecurityContextType(SecurityContextType.valueOf(securityContextType));
<<<<<<< HEAD

=======
		
>>>>>>> upstream/master
		if ("BASIC".equals(securityContextType) ||
				"DIGEST".equals(securityContextType)
				) {
					if (ssl != null && Boolean.parseBoolean(ssl)) {
						if (sslMutualAuth != null && Boolean.parseBoolean(sslMutualAuth)) {
							/*2 way ssl changes*/
							KeyStore clientKeyStore = null;
							try {
								clientKeyStore = KeyStore.getInstance("PKCS12");
							} catch (KeyStoreException e) {
								
								throw new RuntimeException("Unable to get default SSLContext: " + e.getMessage(), e);
							}
					        TrustManager[] trust = new TrustManager[] { new SimpleX509TrustManager()};
							
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
								if (tlsVersion != null && tlsVersion.trim().length() > 0 ) {
									sslContext = SSLContext.getInstance(tlsVersion);
								}
								else {
									sslContext = SSLContext.getInstance("TLSv1.2");
								}
							} catch (NoSuchAlgorithmException e) {
								
								throw new RuntimeException("Unable to configure custom SSL connection:" + e.getMessage(), e);
							}
					        try {
								sslContext.init(key, trust, null);
							} catch (KeyManagementException e) {
								throw new RuntimeException("Unable to configure custom SSL connection:" + e.getMessage(), e);
							}
							clientConfig.setSslContext(sslContext);
						}
						else {/*1wayssl*/
							TrustManager[] trust = new TrustManager[] { new SimpleX509TrustManager()};
							try {
									if (tlsVersion != null && tlsVersion.trim().length() > 0 ) {
										sslContext = SSLContext.getInstance(tlsVersion);
									}
									else {
										sslContext = SSLContext.getInstance("TLSv1.2");
									}
								} catch (NoSuchAlgorithmException e) {
								throw new RuntimeException("Unable to configure custom SSL connection: " + e.getMessage(), e);
							}
							try {
								sslContext.init(null, trust, null);
							}catch (KeyManagementException e) {
								throw new RuntimeException("Unable to configure custom SSL connection:" + e.getMessage(), e);
							}
							clientConfig.setSslContext(sslContext);
						}
					} /* End of if ssl */
			}
		return clientConfig;
	}
}
