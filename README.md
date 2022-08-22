# MarkLogic Kafka connector

The MarkLogic Kafka connector is a [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) 
sink connector for receiving messages from Kafka topics and writing them to a MarkLogic database. 

This page describes how to obtain the MarkLogic Kafka connector and use with either Confluent Platform or Apache Kafka.
For developing and testing the MarkLogic Kafka connector, please see the [guide on contributing](./CONTRIBUTING.md).
That guide also includes detailed instructions on how to use both Confluent Platform and Apache Kafka which may be 
useful if you are new to either product. 

## Requirements

* MarkLogic 9 or higher
* Kafka 2.5 or higher, or Confluent Platform 7 or higher

The MarkLogic Kafka connector may work on versions of Kafka prior to 2.5 or Confluent Platform prior to 7, but it has 
not been tested on those. 

## Using the connector with Confluent Platform

[Confluent Platform](https://docs.confluent.io/platform/current/platform.html) provides an easy mechanism for running
Kafka via a single platform, which includes a simple process for installing new connectors. 

The MarkLogic Kafka connector can be installed via the instructions at 
[Confluent Hub](https://www.confluent.io/hub/marklogic/kafka-marklogic-connector). After installing it, you can use 
[Confluent Control Center](https://docs.confluent.io/platform/current/control-center/index.html) to load and configure
as many instances of the MarkLogic Kafka connector that you wish. See the section below on configuring the connector for
a list of available properties.

## Using the connector with Apache Kafka

For a regular installation of Apache Kafka, obtain the latest version of the MarkLogic Kafka connector from 
[this repository's Releases page](https://github.com/marklogic-community/kafka-marklogic-connector/releases). Download
the jar file - named `kafka-connect-marklogic-(version).jar` - and copy it to the `./libs` directory in your Kafka 
distribution. 

Next, copy the `config/marklogic-connect-standalone.properties` and `config/marklogic-sink.properties` files in this 
repository to the `./config` directory in your Kafka distribution. Modify these properties as necessary; a 
description of each can be found below. 

You can then start up a [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) process. Kafka
Connect will instantiate the MarkLogic Kafka connector based on the configuration in the files above. 

## Configuring the connector

### Common Kafka connector properties

Common Kafka connector properties are defined via the `config/marklogic-connect-standalone.properties` file. 

| Property | Default Value | Description |
|:-------- |:--------------|:------------|
| bootstrap.servers  | 9092  | Points to the Kafka server and port |
| key.converter | org.apache.kafka.connect.storage.StringConverter | Controls the format of the data that will be written to Kafka for source connectors or read from Kafka for sink connectors. |
| value.converter  | org.apache.kafka.connect.storage.StringConverter | Controls the format of the data that will be written to Kafka for source connectors or read from Kafka for sink connectors. |
| key.converter.schemas.enable  | false | Controls the use of schemas for keys |
| value.converter.schemas.enable | false | Controls the use of schemas for values |
| offset.storage.file.filename   | /tmp/connect.offsets | The file to store connector offsets in. By storing offsets on disk, a standalone process can be stopped and started on a single node and resume where it previously left off. |
| offset.flush.interval.ms  | 10000  | Interval at which to try committing offsets for tasks. |

### MarkLogic Kafka connector properties

Properties specific to the MarkLogic Kafka connector are defined via the `config/marklogic-sink.properties` file.

The default values in the table below are enforced by the connector. The `config/marklogic-sink.properties` file 
included in this repository defines values for additional properties for the sole purpose of simplifying testing of the 
connector.

| Property | Default Value  | Description |
|:---------|:------------|:---------------|
| name | marklogic-sink | The name of the connector |
| connector.class | <div>com.marklogic.kafka.</div><div>connect.sink.</div>MarkLogicSinkConnector | The fully-qualified name of the connector class |
| tasks.max | 1 | The maximum number of concurrent tasks |
| topics | marklogic | Comma-separated list of topics to subscribe to |
| ml.connection.host | | Required; a MarkLogic host to connect to. By default, the connector uses the Data Movement SDK, and thus it will connect to each of the hosts in a cluster.|
| ml.connection.port | | Required; the port of a REST API server to connect to |
| ml.connection.securityContextType | DIGEST | Required; the authentication scheme used by the server defined by ml.connection.port; either 'DIGEST', 'BASIC', 'CERTIFICATE', 'KERBEROS', or 'NONE' |
| ml.connection.username | | MarkLogic username for 'DIGEST' and 'BASIC' authentication |
| ml.connection.password | | MarkLogic password for 'DIGEST' and 'BASIC' authentication |
| ml.connection.certFile | | Path to PKCS12 file for 'CERTIFICATE' authentication |
| ml.connection.certPassword | | Password for PKCS12 file for 'CERTIFICATE' authentication |
| ml.connection.externalName | | External name for 'KERBEROS' authentication |
| ml.connection.database | | Name of a database to connect to. If your REST API server has a content database matching that of the one that you want to write documents to, you do not need to set this. |
| ml.connection.type | | Set to 'GATEWAY' when the host identified by ml.connection.host is a load balancer. See https://docs.marklogic.com/guide/java/data-movement#id_26583 for more information. |
| ml.connection.simpleSsl | false | Set to 'true' for a simple SSL strategy that uses the JVM's default SslContext and X509TrustManager and an 'any' host verification strategy |
| ml.connection.enableCustomSsl | false | Set to 'true' to customize how an SSL connection is created. Only supported if securityContextType is 'BASIC' or 'DIGEST'. |
| ml.connection.customSsl.tlsVersion | TLSv1.2 | The TLS version to use for custom SSL |
| ml.connection.customSsl.hostNameVerifier | ANY | The host verification strategy for custom SSL; either 'ANY', 'COMMON', or 'STRICT' |
| ml.connection.customSsl.mutualAuth | false | Set this to true for 2-way SSL; defaults to 1-way SSL |
| ml.document.format | | Specify the format of each document; either 'JSON', 'XML', 'BINARY', 'TEXT', or 'UNKNOWN'. If not set, MarkLogic will determine the document type based on the ml.document.uriSuffix property. |
| ml.document.collections | | Comma-separated list of collections that each document should be written to |
| ml.document.permissions | | Comma-separated list of roles and capabilities that define the permissions for each document |
| ml.document.uriPrefix | | Prefix to prepend to each URI; the URI itself is a UUID |
| ml.document.uriSuffix | | Suffix to append to each URI; will determine the document type if ml.document.format is not set |
| ml.document.temporalCollection | | Name of a temporal collection for documents to be inserted into |
| ml.document.addTopicToCollections | false | Set this to true so that the name of the topic that the connector reads from is added as a collection to each document inserted by the connector |
| ml.document.mimeType | | Specify a mime type for each document; typically ml.document.format will be used instead of this |
| ml.id.strategy | | Set the strategy for generating a unique URI for each document written to MarkLogic. Defaults to 'UUID'. Other choices are: 'JSONPATH', 'HASH', 'KAFKA_META_HASHED', and 'KAFKA_META_WITH_SLASH'. |
| ml.id.strategy.paths | | For use with 'JSONPATH' and 'HASH'; comma-separated list of paths for extracting values for the ID |
| ml.dmsdk.batchSize | 100 | Sets the number of documents to be written in a batch to MarkLogic. This may not have any impact depending on how the connector receives data from Kafka. The connector calls flushAsync on the DMSDK WriteBatcher after processing every collection of records. Thus, if the connector never receives at one time more than the value of this property, then the value of this property will have no impact. |
| ml.dmsdk.threadCount | 8 | Sets the number of threads used for parallel writes to MarkLogic. Similar to the batch size property above, this may never come into play depending on how many records the connector receives at once. |
| ml.dmsdk.transform | | Name of a REST transform to use when writing documents |
| ml.dmsdk.transformParams | | Delimited set of transform parameter names and values; example = param1,value1,param2,value2 |
| ml.dmsdk.transformParamsDelimiter | , | Delimiter for transform parameter names and values |
| ml.log.record.key | false | Set to true to log at the info level the key of each record |
| ml.log.record.headers | false | Set to true to log at the info level the headers of each record | 
| ml.datahub.flow.name | | Name of a Data Hub Framework flow to run after writing documents |
| ml.datahub.flow.steps | | Comma-delimited list of step numbers in a flow to run |
| ml.datahub.flow.logResponse | | Set to true to log at the info level the response data from running a flow |

