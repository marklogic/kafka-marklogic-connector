package com.marklogic.kafka.connect.sink;

import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import kafka.server.KafkaConfig$;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.SendKeyValues;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static net.mguenther.kafka.junit.EmbeddedConnectConfig.kafkaConnect;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.newClusterConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WriteFromKafkaTest extends AbstractIntegrationSinkTest {

    private final String ML_COLLECTION = "kafka-data";
    private final String TOPIC = "test-topic";
    private final String KEY = String.format("key-%s", UUID.randomUUID());

    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void setupKafka() {
        provisionKafkaWithConnectAndMarkLogicConnector();
        kafka.start();
    }

    @AfterEach
    void tearDownKafka() {
        kafka.stop();
    }

    @Test
    void shouldWaitForKeyedRecordsToBePublished() throws InterruptedException {
        Integer NUM_RECORDS = 2;
        sendSomeJsonMessages(NUM_RECORDS);
        retryIfNotSuccessful(() -> assertMarkLogicDocumentsExistInCollection(ML_COLLECTION, NUM_RECORDS,
            format("Expected to find %d records in the ML database", NUM_RECORDS)));
    }

    private void provisionKafkaWithConnectAndMarkLogicConnector() {
        kafka = provisionWith(
            newClusterConfig()
                .configure(
                    kafkaConnect()
                        .deployConnector(connectorConfig(TOPIC, KEY))
                        .with(KafkaConfig$.MODULE$.NumPartitionsProp(), "5")
                )
        );
    }

    private Properties connectorConfig(final String topic, final String key) {
        return MarkLogicSinkConnectorConfigBuilder.create()
            .withTopic(topic)
            .withKey(key)
            .with(MarkLogicSinkConfig.CONNECTION_HOST, testConfig.getHost())
            .with(MarkLogicSinkConfig.CONNECTION_PORT, testConfig.getRestPort())
            .with(MarkLogicSinkConfig.CONNECTION_USERNAME, testConfig.getUsername())
            .with(MarkLogicSinkConfig.CONNECTION_PASSWORD, testConfig.getPassword())
            .with(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, ML_COLLECTION)
            .with(MarkLogicSinkConfig.DMSDK_BATCH_SIZE, 1)
            .build();
    }

    private void sendSomeJsonMessages(Integer numberOfRecords) throws InterruptedException {
        List<KeyValue<String, String>> records = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            records.add(new KeyValue<>("aggregate", "{\"A\": \"" + i + "\"}"));
        }
        kafka.send(SendKeyValues.to(TOPIC, records));
    }

    private void assertMarkLogicDocumentsExistInCollection(String collection, Integer numRecords, String message) {
        StructuredQueryBuilder qb = new StructuredQueryBuilder();
        StructuredQueryDefinition queryDefinition = qb.collection(collection);
        SearchHandle results = getDatabaseClient().newQueryManager().search(queryDefinition, new SearchHandle());
        assertEquals(numRecords.longValue(), results.getTotalResults(), message);
    }
}
