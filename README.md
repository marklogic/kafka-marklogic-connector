# MarkLogic Kafka Connector User Guide

The MarkLogic Kafka connector is a [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) 
sink and source connector. It supports writing data from a topic to a MarkLogic database as well as reading data from
a MarkLogic database via an [Optic query](https://docs.marklogic.com/guide/app-dev/OpticAPI) and sending the result to a topic.

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

Next, copy the `config/marklogic-connect-standalone.properties`, `config/marklogic-sink.properties` and/or 
`config/marklogic-source.properties` files in this repository to the `./config` directory in your Kafka distribution.
These three files are required when running Kafka Connect for the following purposes:

1. `marklogic-connect-standalone.properties` defines several properties required by Kafka Connect
2. `marklogic-sink.properties` defines properties specific to the MarkLogic Kafka sink connector (include this file when
retrieving records from a Kafka topic and sending them to MarkLogic)
3. `marklogic-source.properties` defines properties specific to the MarkLogic Kafka source connector (include this file
when retrieving records from MarkLogic and submitting them to a Kafka topic)

You can then start up a [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) process. Kafka
Connect will instantiate the MarkLogic Kafka connector based on the configuration in the files above. 


## Configuring the connector

Using the MarkLogic Kafka connector requires configuring a set of properties to control how the connector interacts 
with MarkLogic. The manner in which you use the MarkLogic connector will determine how you configure the connector:

- If you are using the connector via Confluent Control Center, the Control Center GUI will list all the connector
  properties with inline help showing a description of each property.
- If you are using the connector by running Kafka Connect from the command line, you'll be configuring a copy of the
  `./config/marklogic-sink.properties` file or `./config/marklogic-source.properties` file in this repository which 
  lists all the connector properties with a description for each one.

When using either the sink or source connector, you'll need to configure two Kafka properties that control
[how data is serialized](https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/) 
when it is sent to the MarkLogic connector. These two properties along with their recommended values are:

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


## Configuring how data is read from MarkLogic

The MarkLogic Kafka connector uses the [Optic API](https://docs.marklogic.com/guide/app-dev/OpticAPI) to read data from 
MarkLogic as rows. Each row is converted into a Kafka `SourceRecord` and sent to a user-defined topic. To enable 
this, the following properties must be configured:

- `ml.source.topic` = the name of a Kafka topic to send records to
- And either `ml.source.optic.dsl` = the [Optic DSL query](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_46710) to execute
- Or `ml.source.optic.serialized` = the [serialized Optic query](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_11208) to execute

The following optional properties can also be configured:

- `ml.source.waitTime` = amount of time, in milliseconds, that the connector will wait on each poll before running the Optic query; defaults to 5000
- `ml.source.optic.outputFormat` = the format of rows returned by the Optic query; defaults to "JSON", and can instead be "XML" or "CSV"
- `ml.source.optic.rowLimit` = the maximum number of rows to retrieve per poll
- `ml.source.optic.constraintColumn.name` = name of a column returned by the Optic query to use for constraining results on subsequent runs
- `ml.source.optic.constraintColumn.uri` = URI of a document that the connector will write after each run that uses a constraint column; the document will contain the highest value in the constraint column
- `ml.source.optic.constraintColumn.collections` = comma-separated list of collection names to assign to the document identified by the URI
- `ml.source.optic.constraintColumn.permissions` = comma-separated list of roles and capabilities to assign to the document identified by the URI

As an example, consider a [TDE template](https://docs.marklogic.com/guide/app-dev/TDE) in an application's schemas
database that defines a view with a schema of "demo" and a view name of "purchases". And assume that rows in this view
should be written to a topic named "marklogic-purchases". A simple configuration for the MarkLogic Kafka connector 
would be:

    ml.source.optic.dsl=op.fromView('demo', 'purchases')
    ml.source.topic=marklogic-purchases

For each row returned by MarkLogic, the MarkLogic Kafka connector will publish a Kafka `SourceRecord` with the following 
data:

- `value` = the row as a string
- `topic` = the topic identified by `ml.source.topic`

The same query can also be executed by defining a serialized version of the plan; this can be useful when the plan has
been constructed already via the MarkLogic Java Client:

    ml.source.optic.serialized={"$optic": {"ns": "op", "fn": "operators", "args": [{"ns": "op", "fn": "from-view", "args": ["demo", "purchases"]}]}}

### Selecting an output type

By default, the connector will return each row as JSON. As mentioned above, it is recommended to configure the Kafka
`value.converter` property with a value of `org.apache.kafka.connect.storage.StringConverter`, which results in the JSON
object being captured as a String in the Kafka `SourceRecord`. You may instead choose to configure the value converter
with a value of `org.apache.kafka.connect.json.JsonConverter`. If you do this, you will either need to configure a 
schema for the connector or configure the Kafka `value.converter.schemas.enable` property with a value of "false".

Each row is represented as a JSON object, with each column represented as an object that defines the type and value of 
the column; example:

```
{
    "Medical.Authors.ID": {
        "type": "xs:integer",
        "value": 2
    },
    "Medical.Authors.LastName": {
        "type": "xs:string",
        "value": "Smith"
    },
    "Medical.Authors.ForeName": {
        "type": "xs:string",
        "value": "Jane"
    },
    "Medical.Authors.Date": {
        "type": "xs:date",
        "value": "2022-05-11"
    },
    "Medical.Authors.DateTime": {
        "type": "xs:dateTime",
        "value": "2022-05-11T10:00:00"
    }
}
```

Note that because the query used the Optic `fromView` accessor with a schema and view name, those schema and view names
are included in the name of each column as well. 

You may instead configure the `ml.source.optic.outputFormat` option and select either "XML" or "CSV". When XML is 
chosen, each row will be an XML element stored as a String in the Kafka `SourceRecord`; example:

```
<t:row xmlns:t="http://marklogic.com/table">
  <t:cell name="Medical.Authors.ID" type="xs:integer">2</t:cell>
  <t:cell name="Medical.Authors.LastName" type="xs:string">Smith</t:cell>
  <t:cell name="Medical.Authors.ForeName" type="xs:string">Jane</t:cell>
  <t:cell name="Medical.Authors.Date" type="xs:date">2022-05-11</t:cell>
  <t:cell name="Medical.Authors.DateTime" type="xs:dateTime">2022-05-11T10:00:00</t:cell>
</t:row>
```

If you choose CSV, each row will be a two-line String, with the first line being the header row, and the second line
containing the values for the row; example:

```
Medical.Authors.ID,Medical.Authors.LastName,Medical.Authors.ForeName,Medical.Authors.Date,Medical.Authors.DateTime
2,Smith,Jane,2022-05-11,2022-05-11T10:00:00
```

### Limiting how many rows are returned

When a Kafka source connector is run, it is required to return an in-memory list of source records. Thus, connector
users have to be careful to ensure that any given run will not return so many source records that the Kafka Connect
process runs out of memory. The MarkLogic Kafka connector is no different in this regard. While MarkLogic can
efficiently return millions of rows or more in a single call, that amount of data being stored in an in-memory list
in the Kafka Connect process is likely to cause memory issues.

The preferred approach for limiting how many rows are returned is via the `ml.source.optic.rowLimit` option, which
can be set to any integer greater than zero. When set, the connector will append an
[Optic limit function](https://docs.marklogic.com/AccessPlan.prototype.limit) call to the end of your query to control
how many rows are returned.

Using this option is strongly recommended when using the constraint column feature described below. If you are not
using a constraint column, you may include a limit function call in your own query. 


### Configuring a constraint column

A common use case when retrieving data from any data store is to only retrieve data that is new or modified since data
was previously retrieved. For example, if an Optic query returns 10k rows on its first run, it is typical that on the 
second run, those same 10k rows should not be returned again. Instead, only rows that are considered to be new or 
modified since the first run should be returned. 

The MarkLogic Kafka connector supports this use case via a "constraint column". When the `ml.source.optic.constraintColumn.name`
column is configured, the connector will perform the on each run:

1. If a maximum value for the constraint column exists from a previous run, the user's query (regardless of whether it's a 
   DSL query or a serialized query) will be enhanced to constrain on only rows whose value for that column is greater 
   than the previously captured max value.
2. After the query returns all of its rows, the connector will immediately query for the new maximum value of the column 
   at the same [MarkLogic server timestamp](https://docs.marklogic.com/guide/rest-dev/intro#id_68993) at which the initial 
   query was run. This value is then used on the next run to constrain the user's query. 
   
A user can choose any column returned by their query, though the following guidelines are recommended:

1. The "greater than" operation should be meaningful on values in the column. In practice, the column will often be a 
   dateTime column or a numeric column (e.g. in the case of a numeric incrementing identifier). But as long as "greater than"
   can be used to select new or modified rows, the column should work properly.
2. The column should ideally contain unique values. For example, if the column contains numbers, an initial 
   query may find that the highest value in the column is 50. If it is possible for future rows to be inserted with a 
   value of 50, those rows will not be returned because the connector will constrain on rows with a value greater than 50.
   Note that a "last updated" dateTime column does not need to contain unique values, assuming that any new row would 
   always have a value greater than any existing row. 
   
Consider the following example of using a constraint column with a row limit:

    ml.source.optic.query=op.fromView('demo', 'purchases').where(op.sqlCondition('price_per_unit > 10'))
    ml.source.optic.rowLimit=100
    ml.source.optic.constraintColumn.name=id

Assuming that 1000 matching rows exist, with IDs from 1 to 1000, then the following will occur:

1. On the first poll, and [orderBy](https://docs.marklogic.com/ModifyPlan.prototype.orderBy) modifier on the given 
   constraint column will be appended to the user's query to ensure that rows with IDs from 1 to 100 are returned. 
2. On the second poll, the user's query will further be modified with a 
   [where](https://docs.marklogic.com/ModifyPlan.prototype.where) function to constrain on rows with an ID greater than 
   100, thus resulting in rows with IDs from 101 to 200 being returned.
3. This behavior will continue until all 1000 rows have been, with each poll returning 100 rows.
4. After all 1000 rows have been returned via 10 poll calls, the next poll will not return any data. 
5. Finally, once rows with an ID greater than 1000 are added to MarkLogic, those will be returned. 


### Configuring storage of a constraint column value

When using the constraint column feature, the MarkLogic Kafka connector defaults to storing the maximum value for 
the constraint column in memory. This can be sufficient for development and test purposes, but it presents a risk where 
if the connector is ever shut down in production, it will retrieve all rows the next time it is run.

To ensure that the constraint column feature works as expected, regardless of what is done to the connector itself, the
connector should be configured to store the maximum constraint column value in MarkLogic itself. To do so, the following
properties (described above) should be configured:

- `ml.source.optic.constraintColumn.uri` = must be configured to enable the feature
- `ml.source.optic.constraintColumn.collections` = optional
- `ml.source.optic.constraintColumn.permissions` = optional, but recommended

When the "uri" property is configured, the connector will store the maximum constraint column value in MarkLogic and 
retrieve it from MarkLogic as well. A user can choose any URI value, though because the connector will store the value
in a JSON document, it is recommended (though not required) to use a suffix of ".json" for the URI. 

Collections can be assigned to the URI if desired via a comma-separated string. 

Unless the Kafka connector is running as an admin user (generally not recommended for security reasons), permissions 
should be configured for the URI to ensure that the appropriate users can read and update the document. Permissions are 
defined as a comma-separated string with alternating roles and capabilities - e.g. 

    ml.source.optic.constraintColumn.permissions=rest-reader,read,rest-writer,update

The document written by the connector will include the maximum constraint column value along with some other metadata
that is intended to be helpful for debugging any problems that arise. However, as of the 1.8.0 release, the contents 
of this document are considered private and thus subject to change with any release. 

### Source connector logging

The manner in which logging is configured for the MarkLogic Kafka connector depends on the particular Kafka distribution
you are using. Generally though, [Kafka uses Log4j](https://docs.confluent.io/platform/current/connect/logging.html) 
to configure logging. Assuming the use of Log4j, `com.marklogic.kafka` is the package to use when configuring logging 
for the MarkLogic Kafka connector. 

If debug logging is not enabled for the connector, then the following items specific to the source connector will be
logged:

- If one or more source records are returned on a poll call, the count of records and the duration of the call will 
be logged at the INFO level
- Any exception thrown during execution of the poll call will have its message logged at the ERROR level 
- Changes to the state of the connector, such as when it is stopped and started, are logged at the INFO level

If debug logging is enabled, the following items will also be logged (at the DEBUG level, unless otherwise stated):

- The beginning of each poll call, along with the value of `ml.source.waitTime`
- The source query that is sent to MarkLogic
- If a constraint column is configured, the query to find the maximum value for this query will be logged along with the
  returned value
- If no records are returned on a poll call, the duration of the call will be logged
- Any exception thrown during execution of the poll call will be logged at the ERROR level and will include the stacktrace


## Configuring how data is written to MarkLogic

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
