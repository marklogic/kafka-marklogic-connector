package com.marklogic.kafka.connect.sinkIntegration;

import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConnector;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.connector.Task;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.FileInputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class KafkaConnectionTest {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConnectionTest.class);

    @Rule
    public KafkaContainer kafkaContainer = new KafkaContainer();

    @ClassRule
    public static GenericContainer alpineContainer =
            new GenericContainer("ml-initialized:9.0-8.2")
                    .withExposedPorts(8000, 8001, 8002);

    @Test
    void testKafkaClientConnectivity() throws Exception {
        kafkaContainer.start();
        String kafkaURL = kafkaContainer.getBootstrapServers();
        logger.info("kafkaURL: " + kafkaURL);
        testKafkaFunctionality(kafkaURL);
    }

    @Test
    void testKafkaConnectConnectivity() throws Exception {
        kafkaContainer.start();
        alpineContainer.start();
        String kafkaURL = kafkaContainer.getBootstrapServers();
        logger.info("kafkaURL: " + kafkaURL);

        Properties appProps = new Properties();
        appProps.load(new FileInputStream("config/marklogic-sink.properties"));
        MarkLogicSinkConfig markLogicSinkConfig = new MarkLogicSinkConfig(appProps);
        Map<String, String> config = markLogicSinkConfig.originalsStrings();
        config.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        config.put(MarkLogicSinkConfig.CONNECTION_DATABASE, "Documents");
        config.put(MarkLogicSinkConfig.CONNECTION_PORT, alpineContainer.getMappedPort(8000).toString());
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "digest");
        config.put(MarkLogicSinkConfig.CONNECTION_USERNAME, "admin");
        config.put(MarkLogicSinkConfig.CONNECTION_PASSWORD, "admin");
        config.put("bootstrap.servers", kafkaURL);

        Map<String, Object> conf = markLogicSinkConfig.originals();
        conf.put("bootstrap.servers", kafkaURL);
        AdminClient adminClient = AdminClient.create(conf);
        NewTopic newTopic = new NewTopic("marklogic", 1, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)
        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(newTopic);
        adminClient.createTopics(newTopics);
        adminClient.close();

        KafkaProducer<String, String> producer = new KafkaProducer<>(
                ImmutableMap.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaURL,
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );
        producer.send(new ProducerRecord<>("marklogic", "testcontainers", "rulezzz")).get();

        Thread.sleep(10000);

//        bootstrap.servers=localhost:9092
//        group.id=test
//        enable.auto.commit=true
//        auto.commit.interval.ms=1000
//        key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
//        value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

        MarkLogicSinkConnector connector = new MarkLogicSinkConnector();
        connector.start(config);
        Task task = connector.taskClass().newInstance();
        task.start(config);

        Thread.sleep(10000);
    }

    protected void testKafkaFunctionality(String bootstrapServers) throws Exception {
        try (
                KafkaProducer<String, String> producer = new KafkaProducer<>(
                        ImmutableMap.of(
                                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                                ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                        ),
                        new StringSerializer(),
                        new StringSerializer()
                );

                KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
                        ImmutableMap.of(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                                ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                        ),
                        new StringDeserializer(),
                        new StringDeserializer()
                );
        ) {
            String topicName = "messages";
            consumer.subscribe(Arrays.asList(topicName));

            producer.send(new ProducerRecord<>(topicName, "testcontainers", "rulezzz")).get();

            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) {
                    return false;
                }

                assertThat(records)
                        .hasSize(1)
                        .extracting(ConsumerRecord::topic, ConsumerRecord::key, ConsumerRecord::value)
                        .containsExactly(tuple(topicName, "testcontainers", "rulezzz"));

                return true;
            });

            consumer.unsubscribe();
        }
    }
}
