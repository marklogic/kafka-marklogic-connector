package com.marklogic.client.state;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaStateSquirrel {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final String topic;
    private final String bootstrapServers;

    public KafkaStateSquirrel(String topic, String bootstrapServers) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
    }

    private Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    public void saveState(String state) throws ExecutionException, InterruptedException {
        try (Producer<Long, String> producer = createProducer()) {
            long time = System.currentTimeMillis();
            final ProducerRecord<Long, String> record =
                new ProducerRecord<>(topic, time, state);
            RecordMetadata metadata = producer.send(record).get();
            producer.flush();
        }
    }

    public void runConsumer() {
        try (Consumer<Long, String> consumer = createConsumer()) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

            for (TopicPartition partition : consumerRecords.partitions()) {
                List<ConsumerRecord<Long, String>> partitionRecords = consumerRecords.records(partition);
                for (ConsumerRecord<Long, String> record : partitionRecords) {
                    logger.info(record.offset() + ": " + record.value());
                }
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                logger.info("Last Offset: " + lastOffset);
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
            }
        }
    }

    public String getLastState() {
        String lastState = null;
        try (Consumer<Long, String> consumer = createConsumer()) {
            consumer.poll(Duration.ofSeconds(10));
            consumer.assignment().forEach((assignment) -> logger.info(assignment.toString()));

            AtomicLong maxTimestamp = new AtomicLong();
            AtomicReference<ConsumerRecord<Long, String>> latestRecord = new AtomicReference<>();

            consumer.endOffsets(consumer.assignment()).forEach((topicPartition, offset) -> {
                logger.info("offset: " + offset);
                consumer.seek(topicPartition, (offset == 0) ? offset : offset - 1);
                consumer.poll(Duration.ofSeconds(10)).forEach(record -> {
                    if (record.timestamp() > maxTimestamp.get()) {
                        maxTimestamp.set(record.timestamp());
                        latestRecord.set(record);
                    }
                });
            });
            if (latestRecord.get() != null) {
                lastState = latestRecord.get().value();
            }
            logger.info(latestRecord.get().toString());
        }
        return lastState;
    }
}
