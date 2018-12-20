package com.marklogic.kafka.connect;

import com.marklogic.client.DatabaseClient;

import java.util.Map;

/**
 * Defines how a map of properties read in by Kafka are used to construct a DatabaseClient. The intent is that
 * a default implementation can use the DatabaseClientConfig object provided by the ml-javaclient-util, with a
 * future implementation using the Bean object in marklogic-client-api, once it's complete. The implementation can
 * also provide different ways for configuring an SSL connection.
 */
public interface DatabaseClientCreator {

	DatabaseClient createDatabaseClient(Map<String, String> kafkaConfig);

}
