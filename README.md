# MarkLogic Kafka connector

The MarkLogic Kafka connector is a [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) 
sink connector for receiving messages from Kafka topics and writing them to a MarkLogic database. 

## Requirements

* MarkLogic 9

## Quick Start with Confluent Platform

TODO, will borrow some content from the CONTRIBUTING file

## Quick Start with Apache Kafka

These instructions assume that you already have an instance of Apache Kafka installed; the [Kafka Quickstart]
(https://kafka.apache.org/quickstart) instructions provide an easy way of accomplishing this. Perform step 1 of these
instructions before proceeding.

Next, if you are running Kafka locally, do the following:

1. Configure `kafkaHome` in gradle-local.properties - e.g. kafkaHome=/Users/myusername/kafka_2.13-2.8.1
1. Run `./gradlew clean deploy` to build a jar and copy it and the below property files into the appropriate Kafka directories

If you are running Kafka on a remote server, do the following:

1. Run `./gradlew clean shadowJar` to build the jar
1. Copy the jar to the <kafkaHome>/libs on the remote server
1. Copy the two properties (config/marklogic-connect-distributed.properties config/marklogic-sink.properties) to <kafkaHome>/config on the remote server

See step 2 in the [Kafka Quickstart guide](https://kafka.apache.org/quickstart) for instructions on starting 
Zookeeper and Kafka. As of August 2022, the guide will instruct you to run the following commands (in separate 
terminal windows, both from the Kafka home directory):

    bin/zookeeper-server-start.sh config/zookeeper.properties

and: 

    bin/kafka-server-start.sh config/server.properties

Next, start the Kafka connector in standalone mode (also from the Kafka home directory):

    bin/connect-standalone.sh config/marklogic-connect-standalone.properties config/marklogic-sink.properties

You'll see a fair amount of logging from Kafka itself; near the end of the logging, look for messages from 
`MarkLogicSinkTask` and MarkLogic Java Client classes such as `WriteBatcherImpl` to ensure that the connector has 
started up correctly.

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

#### Connector-specific properties are defined in config/marklogic-connect-standalone.properties
| Property | Default Value | Description |
|:-------- |:--------------|:------------|
| bootstrap.servers              | 9092                                             | This points to the Kafka server and port                                                                                                                                      |
| key.converter                  | org.apache.kafka.connect.storage.StringConverter | This controls the format of the data that will be written to Kafka for source connectors or read from Kafka for sink connectors.                                              |
| value.converter                | org.apache.kafka.connect.storage.StringConverter | This controls the format of the data that will be written to Kafka for source connectors or read from Kafka for sink connectors.                                              |
| key.converter.schemas.enable   | false                                            | Control the use of schemas for keys                                                                                                                                           |
| value.converter.schemas.enable | false                                            | Control the use of schemas for values                                                                                                                                         |
| offset.storage.file.filename   | /tmp/connect.offsets                             | The file to store connector offsets in. By storing offsets on disk, a standalone process can be stopped and started on a single node and resume where it previously left off. |
| offset.flush.interval.ms       | 10000                                            | Interval at which to try committing offsets for tasks.                                                                                                                        |

#### MarkLogic-specific properties are defined in config/marklogic-sink.properties
| Property                          | Default Value                                                      | Description |
|:----------------------------------|:-------------------------------------------------------------------|:------------|
| name                              | marklogic-sink                                                     | The name of the connector |
| connector.class                   | <div>com.marklogic.kafka.connect.</div>sink.MarkLogicSinkConnector | The FQ name of the connector class |
| tasks.max                         | 1                                                                  | The maximum number of concurrent tasks |
| topics                            | marklogic                                                          | The name of the topic(s) to subscribe to |
| ml.connection.host                | localhost                                                          | A MarkLogic host to connect to. The connector uses the Data Movement SDK, and thus it will connect to each of the hosts in a cluster. |
| ml.connection.port                | 8000                                                               | The port of a REST API server to connect to. |
| ml.connection.database            | Documents                                                          | Optional - the name of a database to connect to. If your REST API server has a content database matching that of the one that you want to write documents to, you do not need to set this. |
| ml.connection.type                | (empty)                                                            | Optional - set to "gateway" when using a load balancer, else leave blank. See https://docs.marklogic.com/guide/java/data-movement#id_26583 for more information. |
| ml.connection.securityContextType | DIGEST                                                             | Either DIGEST, BASIC, CERTIFICATE, KERBEROS, or NONE |
| ml.connection.username            | admin                                                              | MarkLogic username |
| ml.connection.password            | admin                                                              | MarkLogic password |
| ml.connection.certFile            | (empty)                                                            | Certificate file for Certificate based authentication |
| ml.connection.certPassword        | (empty)                                                            | Certificate password for Certificate based authentication |
| ml.connection.externalName        | (empty)                                                            | The external name to use to connect to MarkLogic |
| ml.connection.simpleSsl           | false                                                              | Set to "true" for a "simple" SSL strategy that uses the JVM's default SslContext and X509TrustManager and a "trust everything" HostnameVerifier. Further customization of an SSL connection via properties is not supported. If you need to do so, consider using the source code for this connector as a starting point. |
| ml.dmsdk.batchSize                | 100                                                                | Sets the number of documents to be written in a batch to MarkLogic. This may not have any impact depending on the connector receives data from Kafka, as the connector calls flushAsync on the DMSDK WriteBatcher after processing every collection of records. Thus, if the connector never receives at one time more than the value of this property, then the value of this property will have no impact. |
| ml.dmsdk.threadCount              | 8                                                                  | Sets the number of threads used by the Data Movement SDK for parallelizing writes to MarkLogic. Similar to the batch size property above, this may never come into play depending on how many records the connector receives at once. |
| ml.document.collections           | kafka-data                                                         | Optional - a comma-separated list of collections that each document should be written to |
| ml.document.addTopicToCollections | false                                                              | Set this to true so that the name of the topic that the connector reads from is added as a collection to each document inserted by the connector |
| ml.document.temporalCollection    | (empty)                                                            | Specify the name of a temporal collection for documents to be inserted into |
| ml.document.format                | JSON                                                               | Optional - specify the format of each document; either JSON, XML, BINARY, TEXT, or UNKNOWN |
| ml.document.mimeType              | (empty)                                                            | Optional - specify a mime type for each document; typically the format property above will be used instead of this |
| ml.document.permissions           | rest-reader,read,rest-writer,update                                | Optional - a comma-separated list of roles and capabilities that define the permissions for each document written to MarkLogic |
| ml.document.uriPrefix             | /kafka-data/                                                       | Optional - a prefix to prepend to each URI; the URI itself is a UUID |
| ml.document.uriSuffix             | .json                                                              | Optional - a suffix to append to each URI |
