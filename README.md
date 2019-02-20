# kafka-connect-marklogic

This is a connector for subscribing to Kafka queues and pushing messages to MarkLogic

## Requirements
* MarkLogic 9

## Quick Start

#### To try this out locally:

1. Configure kafkaHome in gradle-local.properties - e.g. kafkaHome=/Users/myusername/tools/kafka_2.11-2.1.0
1. Run "./gradlew deploy" to build a jar and copy it and the below property files into the appropriate Kafka directories

#### To try this out on a remote Kafka server
1. Run "./gradlew jar" to build the jar.
1. Copy the jar to the <kafkaHome>/libs on the remote server.
1. Copy the two properties (config/marklogic-connect-distributed.properties config/marklogic-sink.properties) to <kafkaHome>/config on the remote server.

See https://kafka.apache.org/quickstart for instructions on starting up Zookeeper and Kafka.

To start the Kafka connector in standalone mode (from the Kafka home directory):

    bin/connect-standalone.sh config/marklogic-connect-standalone.properties config/marklogic-sink.properties

You'll see a fair amount of logging from Kafka itself; near the end of the logging, look for messages from 
MarkLogicSinkTask and the Data Movement classes such as WriteBatcherImpl to ensure that the connector has started up
correctly.

You can also start the connector in distributed mode:

    bin/connect-distributed.sh config/marklogic-connect-distributed.properties config/marklogic-sink.properties

The default topic is "marklogic", so to send some messages to that topic, run the following:

    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic marklogic

Be sure that the messages you send are consistent with your configuration properties - i.e. if you've set a format of 
JSON, you should send properly formed JSON objects.

When a document is received and written by the connector, you'll see logging like this:

```
[2018-12-20 12:54:13,561] INFO flushing 1 queued docs (com.marklogic.client.datamovement.impl.WriteBatcherImpl:549)
```

## Configuring the connector

Connector-specific properties are defined in config/marklogic-sink.properties. Please see that file for a list
of all of the properties along with a description of each one.

[Descriptions of the available properties](Properties.html)