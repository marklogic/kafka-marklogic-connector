# kafka-marklogic-connector

This is a connector for subscribing to Kafka queues and pushing messages to MarkLogic

## Requirements
* MarkLogic 9

## Quick Start

* Under Construction

To try this out locally:

1. Configure kafkaHome in gradle-local.properties
1. Run "./gradlew deploy" to build a jar and copy it and the below property files into the appropriate Kafka directories

See https://kafka.apache.org/quickstart for instructions on starting up Zookeeper and Kafka.

To start the Kafka connector in standalone mode (from the Kafka home directory):

    bin/connect-standalone.sh config/marklogic-connect-standalone.properties config/marklogic-sink.properties

Or to start it in distributed mode:

    bin/connect-distributed.sh config/marklogic-connect-distributed.properties config/marklogic-sink.properties

The default topic is "marklogic", so to send some messages to that topic, run the following:

    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic marklogic
