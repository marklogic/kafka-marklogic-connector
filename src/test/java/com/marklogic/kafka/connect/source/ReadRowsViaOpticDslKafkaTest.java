/*
 * Copyright (c) 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

class ReadRowsViaOpticDslKafkaTest extends AbstractIntegrationSourceTest {

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

    @SuppressWarnings("java:S2699") // The assertion happens via kafka.observe
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
