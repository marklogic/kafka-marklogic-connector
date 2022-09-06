# MarkLogic Kafka Connector User Guide

The MarkLogic Kafka connector is a [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) 
sink connector for receiving messages from Kafka topics and writing them to a MarkLogic database. This connector allows
for data to be gathered by Kafka from a variety of sources and then written to MarkLogic without any coding 
required. 

This user guide describes how to obtain the MarkLogic Kafka connector and how to use it with either Confluent Platform or Apache Kafka.
For developing and testing the MarkLogic Kafka connector, please see the [guide on contributing](./CONTRIBUTING.md).
That guide also includes detailed instructions on how to use both Confluent Platform and Apache Kafka which may be 
useful if you are new to either product. Otherwise, it is expected that if you are considering usage of the MarkLogic
Kafka connector, you already have a basic level of understanding of Kafka and/or Confluent Platform.

## Requirements

The MarkLogic Kafka connector has the following system requirements:

* MarkLogic 9 or higher
* Kafka 2.5 or higher, or Confluent Platform 7 or higher

The MarkLogic Kafka connector may work on versions of Kafka prior to 2.5 or Confluent Platform prior to 7, but it has 
not been tested on those. 

## Installation Guide

Apache Kafka can be run either as a standalone product or via a provider such as Confluent. This guide provides 
instructions on using the MarkLogic connector with either Kafka as a standalone product or via Confluent. Other 
Kafka providers exist and should be capable of utilizing the MarkLogic connector as well, but instructions on those 
are out of scope of this document. 

### Using the connector with Confluent Platform

[Confluent Platform](https://docs.confluent.io/platform/current/platform.html) provides an easy mechanism for running
Kafka via a single application, which includes a simple process for installing new connectors. 

The MarkLogic Kafka connector can be installed via the instructions at 
[Confluent Hub](https://www.confluent.io/hub/marklogic/kafka-marklogic-connector). After installing it, you can use 
[Confluent Control Center](https://docs.confluent.io/platform/current/control-center/index.html) to load and configure
as many instances of the MarkLogic Kafka connector that you wish.

### Using the connector with Apache Kafka

For a regular installation of Apache Kafka, obtain the latest version of the MarkLogic Kafka connector from 
[this repository's Releases page](https://github.com/marklogic-community/kafka-marklogic-connector/releases). Download
the jar file - named `kafka-connect-marklogic-(version).jar` - and copy it to the `./libs` directory in your Kafka 
distribution. 

Next, copy the `config/marklogic-connect-standalone.properties` and `config/marklogic-sink.properties` files in this 
repository to the `./config` directory in your Kafka distribution. These two files are required when running Kafka 
Connect for the following purposes:

1. `marklogic-connect-standalone.properties` defines several properties required by Kafka Connect
2. `marklogic-sink.properties` defines properties specific to the MarkLogic Kafka connector

You can then start up a [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) process. Kafka
Connect will instantiate the MarkLogic Kafka connector based on the configuration in the files above. 


## Configuring the connector

Using the MarkLogic Kafka connector requires configuring a set of properties to control how Kafka passes data to 
the connector, how the connector connects to MarkLogic, and how data is written to MarkLogic. 

The manner in which you use the MarkLogic connector will determine how you configure the connector:

- If you are using the connector via Confluent Control Center, the Control Center GUI will list all the connector
  properties with inline help showing a description of each property.
- If you are using the connector by running Kafka Connect from the command line, you'll be configuring a copy of the
  `./config/marklogic-sink.properties` file in this repository which also lists all the connector properties with a 
  description for each one.

In addition to the properties found in the locations above, you'll also need to configure two Kafka properties that 
control [how data is serialized](https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/) 
when it is sent to the MarkLogic connector. These two properties along with their required values are:

- `key.converter=org.apache.kafka.connect.storage.StringConverter`
- `value.converter=org.apache.kafka.connect.storage.StringConverter`

Again, the manner in which you use the MarkLogic connector will determine how you configure the above two properties:

- If you are using the connector via Confluent Control Center, the Control Center GUI will allow you to configure these
two properties in the "Common" section
- If you are using the connector by running Kafka Connect from the command line, you'll configure these properties in a
copy of the `./config/marklogic-connect-standalone.properties` file. This file already has the two properties set to the
  correct values, so you should not need to do anything further with them.


## Configuring the connection

You must configure the connector to control how it connects to MarkLogic. The properties you must configure are determined 
by the type of authentication required by the MarkLogic app server that you connect to and whether it requires SSL usage.

Regardless of the required authentication strategy, you must configure the following properties:

- `ml.connection.host` = the name of a host in the MarkLogic cluster you wish to connect to
- `ml.connection.port` = the port of the MarkLogic app server you wish to connect to
- `ml.connection.securityContextType` = the authentication strategy required by the MarkLogic app server; defaults to DIGEST

The choices for `ml.connection.securityContextType` are DIGEST, BASIC, CERTIFICATE, KERBEROS, and NONE. The additional
properties required for each are described in the following sections.

### Configuring digest and basic authentication

Both digest and basic authentication require the following properties to be configured:

- `ml.connection.username` = the name of the MarkLogic user to authenticate as
- `ml.connection.password` = the password of the MarkLogic user

### Configuring certificate authentication

Certificate authentication requires the following properties to be configured:

- `ml.connection.certFile` = path to a PKCS12 certificate file
- `ml.connection.certPassword` = password for the PKCS12 certificate file

### Configuring Kerberos authentication

Kerberos authentication requires the following property to be configured:

- `ml.connection.externalName` = the name of the principal to be used in Kerberos authentication

### Configuring no authentication

A value of "NONE" is used when the MarkLogic app server uses "application-level" authentication. No additional
properties need to be required for this authentication strategy.

### Configuring SSL usage

The MarkLogic connector provides two options for configuring the usage of SSL when connecting to MarkLogic:

- `ml.connection.simpleSsl` = `true` to use a "trust-everything" approach; this is useful during development for easier testing
- `ml.connection.enableCustomSsl` = `true` to utilize other properties for a custom SSL strategy

If a custom SSL approach is used, you can use the following properties to configure this approach:

- `ml.connection.customSsl.tlsVersion` = the TLS version to use for constructing an `SSLContext`. Defaults to `TLSv1.2`.
- `ml.connection.customSsl.mutualAuth` = `true` to configure mutual, or "2-way", SSL authentication

If `ml.connection.customSsl.mutualAuth` is set to `true`, you must also configure these properties:

- `ml.connection.certFile` = path to a PKCS12 key store
- `ml.connection.certPassword` = password for the PKCS12 key store


## Configuring how data is written

By default, the MarkLogic Kafka connector assumes that the app server associated with the port defined by the `ml.connection.port`
property is a [REST API app server](https://docs.marklogic.com/guide/rest-dev) - that is, the value of its `url rewriter`
property is `/MarkLogic/rest-api/rewriter.xml` or a variation of that rewriter. This allows the MarkLogic connector to use the 
[MarkLogic Data Movement SDK](https://developer.marklogic.com/learn/data-movement-sdk/), also known as DMSDK, to 
efficiently write batches of documents to MarkLogic without any custom coding required. The connector user can ideally 
meet all of their ingestion requirements simply by configuring properties that control how data is written. And in the
event that some transformation of the data is required, a MarkLogic REST transform can be specified to perform 
code-driven transformations. 

However, if you find that are not able to meet your requirements via the connector properties and optionally with a REST
transform, you may instead configure the connector to write data via a custom 
[Bulk Data Services endpoint](https://github.com/marklogic/java-client-api/wiki/Bulk-Data-Services). Bulk Data Services
is intended to integrate with dataflow frameworks like Kafka and provide endpoint developers with complete control over
how data is processed. This approach requires expertise with implementing a Bulk Data Services endpoint and is thus 
recommended only for users with experience in writing and deploying custom code to MarkLogic.


### Writing data via configuration (DMSDK)

The intent behind using DMSDK with the MarkLogic REST API is that as many aspects of writing data to MarkLogic can be 
controlled via properties without the need to write any code. The following sections describe the various ways in 
which this can be achieved.

#### Configuring document URIs

Documents in a MarkLogic database are uniquely identified by a URI. By default, a UUID is generated and used as the URI
of each document written to MarkLogic. The following properties allow for this to be customized:

- `ml.document.uriPrefix` = a string to be prepended to each URI
- `ml.document.uriSuffix` = a string to be appended to each URI

#### Configuring document metadata

The following properties control how each document is written:

- `ml.document.format` = the format of the written document; either JSON, XML, BINARY, TEXT, or UNKNOWN
- `ml.document.collections` = a comma-separated list of [collections](https://docs.marklogic.com/guide/search-dev/collections) 
  that each document should be included in
- `ml.document.permissions` = a comma-separated list of [permissions](https://docs.marklogic.com/guide/security/permissions) 
  that will be added to each document; formatted as role1,capability1,role2,capability2,etc
- `ml.document.temporalCollection` = name of a [temporal collection](https://docs.marklogic.com/guide/temporal/intro) 
  that each document should be included in
- `ml.document.addTopicToCollections` = `true` if the name of the topic associated with the connector should be used as
  a collection for each document
- `ml.document.mimeType` = the MIME type for each document; this is an alternative to `ml.document.format` for specifying 
  the format of each document

#### Configuring a REST transform

A [MarkLogic REST transform](https://docs.marklogic.com/guide/rest-dev/transforms) provides a mechanism for writing 
custom code to transform a document before it is written, which is otherwise not possible to do via the existing set of 
connector properties. 

To use a transform, configure the following properties:

- `ml.dmsdk.transform` = the name of the REST transform to use
- `ml.dmsdk.transformParams` = comma-separated list of transform parameter names and values; e.g. param1,value1,param2,value2
- `ml.dmsdk.transformParamsDelimiter` = the delimiter for `ml.dmsdk.transformParams`; defaults to a comma

If you are using [ml-gradle](https://github.com/marklogic-community/ml-gradle) to manage your MarkLogic project, you 
can put REST transforms in the `src/main/ml-modules/transforms` directory in your project.

#### Including Kafka metadata

Each Kafka record passed to the MarkLogic connector has several pieces of metadata that can be useful both for 
including in written documents and for debugging when a record fails to be written. Kafka metadata can be included in 
each document by configuring the following property:

- `ml.dmsdk.includeKafkaMetadata` = `true` to include Kafka metadata

This will result in the following pieces of Kafka record metadata being in each associated document as MarkLogic 
[document metadata values](https://docs.marklogic.com/guide/java/document-operations#id_83108):

- `kafka.topic` = the Kafka topic associated with the record
- `kafka.key` = the key of the Kafka record (may be null)
- `kafka.offset` = the offset of the Kafka record
- `kafka.partition` = the partition of the Kafka record
- `kafka.timestamp` = the timestamp of the Kafka record

#### Configuring DMSDK performance

The performance of how data is written to MarkLogic can be configured via the following properties:

- `ml.dmsdk.batchSize` = the number of documents written in one call to MarkLogic; defaults to 100
- `ml.dmsdk.threadCount` = the number of threads used for making calls to MarkLogic; defaults to 8

The size of each batch will only have an impact if Kafka is sending a number of records greater than the batch size 
each time it sends data to the MarkLogic connector. For each collection of records that the MarkLogic connector 
receives, it will perform an asynchronous flush of documents to MarkLogic. Thus, if the connector is using a batch size 
of 100 and it receives 10 records from Kafka, it will immediately (but asynchronously) write those to MarkLogic. If 
Kafka never sends at least 100 records to the connector, then increasing the batch size will not have any impact. 
Information on how Kafka can be configured to control how much data it sends to a connector is specific to a Kafka 
installation; please see the documentation for your Kafka distribution for more information. 

Similar to the batch size, the number of threads used by the connector will only have an impact if the connector is 
being called frequently enough by Kafka to use multiple threads. This property has some overlap with a generic Kafka 
connector property named `tasks.max`, which effectively controls how many instances of the MarkLogic connector are
instantiated. Depending on the amount of data you are trying to send from Kafka to MarkLogic, you may want to raise 
both of these values to maximize performance. The [MarkLogic Monitoring dashboard](https://docs.marklogic.com/guide/monitoring/intro) 
is helpful in understanding MarkLogic resource consumption and whether changing these values has any impact on performance. 

#### Configuring a DHF flow to be run

The MarkLogic connector includes support for running a [Data Hub Framework (DHF)](https://docs.marklogic.com/datahub) 
flow after a batch of documents is written to MarkLogic. The following properties can be used to configure this feature:

- `ml.datahub.flow.name` = name of the flow to run
- `ml.datahub.flow.steps` = comma-separated list of step numbers in the flow to run
- `ml.datahub.flow.logResponse` = `true` if the JSON response generated by DHF should be logged by the connector

Note that only "query" steps should be run. Running a DHF ingestion step typically will not be meaningful as an 
ingestion step depends on reading data from a filesystem. You can however run an ingestion step by configuring a REST
transform as described above. Please see the DHF documentation for information on how to configure the DHF REST transform
for running an ingestion step. 


### Writing data via custom code (Bulk Data Services)

MarkLogic's [Bulk Data Services](https://github.com/marklogic/java-client-api/wiki/Bulk-Data-Services) feature is 
intended to give developers complete control over how data is written to MarkLogic via an external program, which is 
typically a dataflow framework like Kafka that can support multiple workers writing to MarkLogic at the same time. The 
MarkLogic Kafka connector utilizes Bulk Data Services to send Kafka record data to a custom endpoint in which a 
developer can write any code they like to control how the data is processed. 

Configuring the MarkLogic Kafka connector to use Bulk Data Services requires the following two properties:

- `ml.sink.bulkds.apiUri` = the URI of the Bulk Data Services API declaration
- `ml.connection.modulesDatabase` = the name of the modules database containing both the API declaration and the 
  endpoint module that it refers to

The MarkLogic Kafka connector expects the API declaration to be:

```
{
  "endpoint": "/path/to/your/endpoint.sjs",
  "params": [
    {
      "name": "endpointConstants",
      "datatype": "jsonDocument",
      "multiple": false,
      "nullable": false
    },
    {
      "name": "input",
      "datatype": "jsonDocument",
      "multiple": true,
      "nullable": true
    }
  ],
  "$bulk": {
    "inputBatchSize": 100
  }
}
```

The `endpoint` field must be the URI of the associated Bulk Data Services endpoint module. You are free to set the 
`inputBatchSize` to any numeric value you want based on the expected workload for your connector. 

It is recommended to start your endpoint module implementation with the following code:

```
'use strict';
declareUpdate();

var input;
var endpointConstants;

const inputSequence = input instanceof Document ? [input] : input;
endpointConstants = fn.head(xdmp.fromJSON(endpointConstants));

for (let item of inputSequence) {
  item = fn.head(xdmp.fromJSON(item));
  // TODO Determine what to do with each item
}
```

As shown in the code above, `input` will either be a single document (if the batch size is 1) or a sequence. Normalizing
this variable into a sequence typically simplifies coding the rest of the endpoint module.

Additionally, the expression `fn.head(xdmp.fromJSON(arg))` is used to convert the arg into a proper JSON object. This
is typically desirable for coding the rest of the endpoint module as well. 

The `input` variable then consists of a JSON object for each Kafka record passed to the connector. That JSON object 
contains the following fields:

- `content` = a string representation of the data in the Kafka record
- `kafkaMetadata` = a JSON object containing the following fields: `topic`, `key`, `offset`, `partition`, and `timestamp` 

The `endpointConstants` variable is a JSON object. It contains one field for every non-empty connector property whose 
name starts with `ml.document.`. These properties are included to support an endpoint developer who wishes to make a 
dynamic endpoint that is driven by the values of these properties instead of them being hardcoded within the 
endpoint module. This can allow for reusing the same endpoint module across multiple connector instances.

Note that when using this approach, none of the properties starting with the following prefixes will have any impact, 
as they are specific to the approach that uses DMSDK:

- `ml.datahub`
- `ml.dmsdk`
- `ml.document`  
- `ml.id.strategy`

As noted above, the `ml.document` properties will be included in `endpointConstants`, but they otherwise have no impact
because the endpoint developer chooses whether to apply them or not in their endpoint module.

#### Configuring Bulk Data Services performance

MarkLogic's Bulk Data Services feature is designed to leverage the multi-threading and parallelization support provided
by existing dataflow frameworks. To achieve parallel writes to MarkLogic and increase performance, you should configure
the generic Kafka connector property named `tasks.max` to control how many Kafka connector tasks will write to MarkLogic
in parallel. 

A key design feature of Bulk Data Services to understand is that, unlike MarkLogic's Data Movement SDK, it does not 
support asynchronous flushing of data. Bulk Data Services will not write any data to MarkLogic until it has a number of
documents equalling that of the batch size in the API declaration. Importantly, partial batches will not be written 
until either enough records are received to meet the batch size, or until Kafka invokes the "flush" operation on the
MarkLogic Kafka connector. You can use the Kafka connector property named `offset.flush.interval` to control how often
the flush operation is invoked. This is a synchronous operation, but you may wish to have this occur fairly regularly, 
such as every 5 or 10 seconds, to ensure that partial batches of data are not waiting too long to be written to 
MarkLogic.

You may also want to configure the `inputBatchSize` field in your API declaration to see if increasing or decreasing
this value results in greater performance. 

As always with MarkLogic applications, use the [MarkLogic Monitoring dashboard](https://docs.marklogic.com/guide/monitoring/intro)
to understand resource consumption and server performance while testing various connector settings. 
