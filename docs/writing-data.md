---
layout: default
title: Writing data
nav_order: 6
---

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
controlled via properties without the need to write any code.

#### Security requirements

The user that the connector authenticates as must have the `rest-reader` and `rest-writer` privileges in order to
write data via the MarkLogic REST API, which the connector depends upon.

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

#### Security requirements

Unlike when using the MarkLogic REST API, no specific privileges or roles are required in order for the connector to
invoke a Bulk Data Services endpoint. Instead, the required privileges and/or roles for the MarkLogic user that the
connector authenticates as will be entirely determined by the Bulk Data Services endpoint implementation.

#### Configuring Bulk Data Services usage

Configuring the MarkLogic Kafka connector to use Bulk Data Services involves the following properties:

- `ml.sink.bulkds.endpointUri` = the URI of the Bulk Data Services endpoint module
- `ml.sink.bulkds.batchSize` = an optional batch size; defaults to 100. Note that if you include `$bulk/inputBatchSize`
  in your API declaration, it will be ignored in favor of this property.

Bulk Data Services requires that your endpoint module have an API declaration. The URI of the API declaration must
match that of your endpoint, but with `.api` as a suffix instead of `.sjs` or `.xqy`. The MarkLogic Kafka connector
expects the API declaration to have the following configuration:

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
  ]
}
```

The `endpoint` field should have the same value as the `ml.sink.bulkds.endpointUri` property.

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
documents equalling that of the `ml.sink.bulkds.batchSize` property. Importantly, partial batches will not be written
until either enough records are received to meet the batch size, or until Kafka invokes the `flush` operation on the
MarkLogic Kafka connector. You can use the Kafka connector property named `offset.flush.interval` to control how often
the flush operation is invoked. This is a synchronous operation, but you may wish to have this occur fairly regularly,
such as every 5 or 10 seconds, to ensure that partial batches of data are not waiting too long to be written to
MarkLogic.

As always with MarkLogic applications, use the [MarkLogic Monitoring dashboard](https://docs.marklogic.com/guide/monitoring/intro)
to understand resource consumption and server performance while testing various connector settings.


### Dead Letter Queue configuration

Starting with version 1.8.0, the sink connector makes use of the dead letter queue (DLQ) if the user has configured
Kafka appropriately. [Please see the Kafka documentation](https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/)
for more information on configuring the dead letter queue in Kafka.

Note that the DLQ is only supported when using DMSDK to write documents to MarkLogic; DLQ support for Bulk Data Services
will be supported in a future release.

When Kafka has been configured to use the DLQ, there are two events in the sink connector that will cause a record to be
sent to the DLQ.
- "Record conversion" : If a specific record cannot be converted into the target format to be delivered to MarkLogic,
  then that record will be sent to the DLQ.
- "Write failure" : If a batch of documents (converted Kafka records from the source topic) fails to be written to
  MarkLogic then each of the records in the batch will be sent to the DLQ. The entire batch must be sent to the DLQ since
  the connector is unable to determine the cause of the failure.

When a record is sent to the DLQ, the connector first adds headers to the record providing information about the cause
of the failure in order to assist with troubleshooting and potential routing.
- "marklogic-failure-type" : Either "Write failure" or "Record conversion"
- "marklogic-exception-message" : Information from MarkLogic when there is a write failure
- "marklogic-original-topic" : The name of the topic that this record came from
- "marklogic-target-uri" : For write failures, this contains the target URI for the document


### Sink connector error handling

The most common cause of errors in the MarkLogic Kafka sink connector is those occurring when a batch of documents is
written to MarkLogic. The connector provides the following support for these errors:

1. The error message and size of the failed batch is logged at the "ERROR" level.
2. If the `ml.dmsdk.includeKafkaMetadata` option is set to "true", then each failed record in the batch will have its
   URI and associated Kafka metadata logged at the "ERROR" level.
3. If Dead Letter Queue (DLQ) support has been configured as described above and DMSDK is used to write documents to
   MarkLogic, then each failed record in the batch will be sent to the user-defined DLQ topic.

The connector also provides support for the rare instance where a Kafka sink record cannot be converted into a
document to be written to MarkLogic. Such an error will be logged at the "ERROR" level, and if DLQ support is enabled
by the user, the sink record will be sent to the DLQ topic.

In addition, always check the MarkLogic server log files, particularly those associated with the MarkLogic app server
that the connector connects to, for additional information about the error.

Finally, it is possible for an unexpected error to occur within the connector. Contrary to a source connector, which is
required to catch any error that occurs, an unexpected error in the sink connector will be thrown and eventually caught
and logged by Kafka. However, nothing will be sent to the user-configured DLQ topic in this scenario as the error will
not be associated with a particular sink record. Kafka and MarkLogic server logs should be examined to determine the
cause of the error. 
