package com.marklogic.kafka.connect.sink;

import com.marklogic.junit5.spring.SimpleTestConfig;
import kafka.server.KafkaConfig$;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static com.marklogic.kafka.connect.sink.AbstractSinkTask.*;
import static net.mguenther.kafka.junit.EmbeddedConnectConfig.kafkaConnect;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.newClusterConfig;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;

public class SendWriteFailureRecordsToDlqKafkaTest extends AbstractIntegrationSinkTest {

    // Declared by AbstractSpringMarkLogicTest
    @Autowired
    private SimpleTestConfig testConfig;

    private final String ML_COLLECTION = "kafka-data";
    private final String TOPIC = "test-topic";
    private final String DLQ_TOPIC = "test-dlq";
    private final String KEY = String.format("key-%s", UUID.randomUUID());
    private final Integer NUM_RECORDS = 1;

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
    void failedBatchesShouldGoToTheDlq() throws InterruptedException {
        sendSomeJsonMessages(NUM_RECORDS);

        assertMessageOnDlqAndHasExpectedHeaders(DLQ_TOPIC, (NUM_RECORDS));
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
            .with(MarkLogicSinkConfig.CONNECTION_USERNAME, "kafka-unprivileged-user")
            .with(MarkLogicSinkConfig.CONNECTION_PASSWORD, "kafkatest")
            .with(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, ML_COLLECTION)
            .with(MarkLogicSinkConfig.DMSDK_THREAD_COUNT, 1)
            .with(MarkLogicSinkConfig.DMSDK_BATCH_SIZE, 100)
            .with("errors.deadletterqueue.topic.name", DLQ_TOPIC)
            .with("errors.deadletterqueue.topic.replication.factor", 1)
            .with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
            .with("errors.tolerance", "all")
            .with("value.converter.schemas.enable", false)
            .build();
    }

    private void sendSomeJsonMessages(Integer numberOfRecords) throws InterruptedException {
        List<KeyValue<String, String>> records = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            records.add(new KeyValue<>("aggregate", "{\"A\": \"" + i + "\"}"));
        }
        kafka.send(SendKeyValues.to(TOPIC, records));
    }

    private void assertMessageOnDlqAndHasExpectedHeaders(String topic, Integer numRecords) throws InterruptedException {
        kafka.observe(on(topic, numRecords));

        Headers headers = kafka.read(ReadKeyValues.from(topic))
            .stream()
            .findFirst()
            .map(KeyValue::getHeaders)
            .orElseThrow(() -> new RuntimeException("No records found."));

        Assertions.assertEquals(MARKLOGIC_WRITE_FAILURE,
            new String(headers.headers(MARKLOGIC_MESSAGE_FAILURE_HEADER).iterator().next().value()),
            "The failure reason on the DLQ message was not what was expected");
        Assertions.assertTrue(
            new String(headers.headers(MARKLOGIC_MESSAGE_EXCEPTION_MESSAGE).iterator().next().value())
                .startsWith("Local message: failed to apply resource at documents: Internal Server Error. Server Message: SEC-COLPERM:"),
            "The exception message on the DLQ message was not what was expected");
        Assertions.assertEquals(TOPIC,
            new String(headers.headers(MARKLOGIC_ORIGINAL_TOPIC).iterator().next().value()),
            "The original topic on the DLQ message was not what was expected");
    }
}
