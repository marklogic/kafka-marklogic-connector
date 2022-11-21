package com.marklogic.kafka.connect.source;

import kafka.server.KafkaConfig$;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static net.mguenther.kafka.junit.EmbeddedConnectConfig.kafkaConnect;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.newClusterConfig;

public class ReadRowsViaOpticDslKafkaTest extends AbstractIntegrationSourceTest {

    private final String ML_OPTIC_DSL = "";
    private final String TOPIC = "test-topic";

    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void setupKafka() {
        provisionKafkaWithConnectAndMarkLogicConnector();
        kafka.start();
    }

    @Override
    // Until I start adding data, I don't want to delete any data
    protected String getJavascriptForDeletingDocumentsBeforeTestRuns() {
        return "declareUpdate(); ";
    }

    @AfterEach
    void tearDownKafka() {
        kafka.stop();
    }

    @Test
    // Place holder for some test code.
    // For now, this test just verifies that the basic Source connector code is in place and is loaded without errors.
    void shouldWaitForKeyedRecordsToBePublished() throws InterruptedException {
        Thread.sleep(5000);
    }

    private void provisionKafkaWithConnectAndMarkLogicConnector() {
        kafka = provisionWith(
            newClusterConfig()
                .configure(
                    kafkaConnect()
                        .deployConnector(sourceConnectorConfig(TOPIC, ML_OPTIC_DSL))
                        .with(KafkaConfig$.MODULE$.NumPartitionsProp(), "5")
                )
        );
    }

    private Properties sourceConnectorConfig(final String topic, final String opticDsl) {
        return MarkLogicSourceConnectorConfigBuilder.create()
            .withTopic(topic)
            .withDsl(opticDsl)
            .with(MarkLogicSourceConfig.CONNECTION_HOST, testConfig.getHost())
            .with(MarkLogicSourceConfig.CONNECTION_PORT, testConfig.getRestPort())
            .with(MarkLogicSourceConfig.CONNECTION_USERNAME, testConfig.getPassword())
            .with(MarkLogicSourceConfig.CONNECTION_PASSWORD, testConfig.getPassword())
            .with(MarkLogicSourceConfig.DMSDK_BATCH_SIZE, 1)
            .build();
    }
}
