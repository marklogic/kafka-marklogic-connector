package com.marklogic.kafka.connect.source;

import kafka.server.KafkaConfig$;
import net.mguenther.kafka.junit.EmbeddedConnectConfig;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.newClusterConfig;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE;

public class ReadRowsViaOpticDslKafkaTest extends AbstractIntegrationSourceTest {

    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void setup() {
        loadFifteenAuthorsIntoMarkLogic();
        setupKafka();
    }

    @AfterEach
    void tearDownKafka() {
        kafka.stop();
    }

    @Test
    void shouldWaitForKeyedRecordsToBePublished() throws InterruptedException {
        kafka.observe(on(AUTHORS_TOPIC, 15));
    }

    void setupKafka() {
        provisionKafkaWithConnectAndMarkLogicConnector();
        kafka.start();
        kafka.createTopic(
            withName(AUTHORS_TOPIC)
            .with(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE)
            .build()
        );
    }

    private void provisionKafkaWithConnectAndMarkLogicConnector() {
        kafka = provisionWith(
            newClusterConfig()
                .configure(
                    EmbeddedConnectConfig.kafkaConnect()
                        .deployConnector(sourceConnectorConfig(AUTHORS_TOPIC, AUTHORS_OPTIC_DSL))
                        .with(KafkaConfig$.MODULE$.NumPartitionsProp(), "5")
                )
        );
    }

    private Properties sourceConnectorConfig(final String topic, final String opticDsl) {
        return MarkLogicSourceConnectorConfigBuilder.create()
            .withTopic(topic)
            .withDsl(opticDsl)
            .with(MarkLogicSourceConfig.WAIT_TIME, 0)
            .with(MarkLogicSourceConfig.CONNECTION_HOST, testConfig.getHost())
            .with(MarkLogicSourceConfig.CONNECTION_PORT, testConfig.getRestPort())
            .with(MarkLogicSourceConfig.CONNECTION_USERNAME, testConfig.getUsername())
            .with(MarkLogicSourceConfig.CONNECTION_PASSWORD, testConfig.getPassword())
            .build();
    }
}
