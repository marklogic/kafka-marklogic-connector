---
layout: default
title: Reading Data
nav_order: 5
---

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

## Overview

The MarkLogic Kafka connector uses the [Optic API](https://docs.marklogic.com/guide/app-dev/OpticAPI) to read data from
MarkLogic as rows. Each row is converted into a Kafka `SourceRecord` and sent to a user-defined topic. To enable
this, the following properties must be configured:

- `ml.source.topic` = the name of a Kafka topic to send records to
- And either `ml.source.optic.dsl` = the [Optic DSL query](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_46710) to execute
- Or `ml.source.optic.serialized` = the [serialized Optic query](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_11208) to execute

The following optional properties can also be configured:

- `ml.source.waitTime` = amount of time, in milliseconds, that the connector will wait on each poll before running the Optic query; defaults to 5000
- `ml.source.optic.keyColumn` = name of a column to use for generating a row for each source record
- `ml.source.optic.outputFormat` = the format of rows returned by the Optic query; defaults to "JSON", and can instead be "XML" or "CSV"
- `ml.source.optic.includeColumnTypes` = set to true so that each record includes column types; defaults to false; only supported when the output format is JSON or XML
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
been constructed in another tool already, such as the MarkLogic Java Client, and can then be exported into its
serialized form:

    ml.source.optic.serialized={"$optic": {"ns": "op", "fn": "operators", "args": [{"ns": "op", "fn": "from-view", "args": ["demo", "purchases"]}]}}

**Warning** - the Kafka `tasks.max` property is ignored by the MarkLogic Kafka source connector. Running 2 or more tasks
with the same configuration would produce 2 or more copies of the same records and may also lead to inconsistent
results when using a constraint column. If you have a valid scenario for setting this property to 2 or higher, please
file an issue with your use case.

## Selecting an output type

By default, the connector will return each row as JSON. As mentioned above, it is recommended to configure the Kafka
`value.converter` property with a value of `org.apache.kafka.connect.storage.StringConverter`, which results in the JSON
object being captured as a String in the Kafka `SourceRecord`. You may instead choose to configure the value converter
with a value of `org.apache.kafka.connect.json.JsonConverter`. If you do this, you will either need to configure a
schema for the connector or configure the Kafka `value.converter.schemas.enable` property with a value of "false".

Each row is represented as a JSON object, as shown in the example below:

```
{
  "Medical.Authors.ID": 1,
  "Medical.Authors.LastName": "Smith",
  "Medical.Authors.ForeName": "Jane",
  "Medical.Authors.Date": "2022-07-13",
  "Medical.Authors.DateTime": "2022-07-13T09:00:00"
}

```

Note that because the query used the Optic `fromView` accessor with a schema and view name, those schema and view names
are included in the name of each column as well.

You may instead configure the `ml.source.optic.outputFormat` option and select either "XML" or "CSV". When XML is
chosen, each row will be an XML element stored as a String in the Kafka `SourceRecord`; example:

```
<t:row xmlns:t="http://marklogic.com/table">
  <t:cell name="Medical.Authors.ID">2</t:cell>
  <t:cell name="Medical.Authors.LastName">Smith</t:cell>
  <t:cell name="Medical.Authors.ForeName">Jane</t:cell>
  <t:cell name="Medical.Authors.Date">2022-05-11</t:cell>
  <t:cell name="Medical.Authors.DateTime">2022-05-11T10:00:00</t:cell>
</t:row>
```

If you choose CSV, each row will be a two-line String, with the first line being the header row, and the second line
containing the values for the row; example:

```
Medical.Authors.ID,Medical.Authors.LastName,Medical.Authors.ForeName,Medical.Authors.Date,Medical.Authors.DateTime
2,Smith,Jane,2022-05-11,2022-05-11T10:00:00
```

## Including column types in source records

If the selected output type for source records is JSON or XML, you may set the `ml.source.optic.includeColumnTypes`
option to "true", which will result in the column type being included for each column in each source record.

For example, when the output type is JSON, the source record will nest both the type and value for each column (note that
this is pretty-printed for readability purposes; the data written to a source record is not pretty-printed):

```
{
  "Medical.Authors.ID" : {
    "type" : "xs:integer",
    "value" : 1
  },
  "Medical.Authors.LastName" : {
    "type" : "xs:string",
    "value" : "Smith"
  },
  "Medical.Authors.ForeName" : {
    "type" : "xs:string",
    "value" : "Jane"
  },
  "Medical.Authors.Date" : {
    "type" : "xs:date",
    "value" : "2022-07-13"
  },
  "Medical.Authors.DateTime" : {
    "type" : "xs:dateTime",
    "value" : "2022-07-13T09:00:00"
  }
}
```

When the output type is XML, the source record will include the column type as an attribute for each column (like the
JSON example above, this too is pretty-printed for readability purposes):

```
<t:row xmlns:t="http://marklogic.com/table">
  <t:cell name="Medical.Authors.ID" type="xs:integer">1</t:cell>
  <t:cell name="Medical.Authors.LastName" type="xs:string">Smith</t:cell>
  <t:cell name="Medical.Authors.ForeName" type="xs:string">Jane</t:cell>
  <t:cell name="Medical.Authors.Date" type="xs:date">2022-07-13</t:cell>
  <t:cell name="Medical.Authors.DateTime" type="xs:dateTime">2022-07-13T09:00:00</t:cell>
</t:row>
```

## Generating a key for each source record

Each source record returned by a Kafka source connector can have a key defined. A key is optional, and thus by default,
the MarkLogic Kafka connector will set the key to `null` on each record it returns.

You may instead choose to set the key to the value of a column in the row associated with a source record. Typically,
this column will either contain the URI of the document that the row is projected from, or it will contain a value that
acts as a unique identifier for the row.

Note that when using an Optic accessor such as `op.fromView`, the column may have the schema and view name prepended
to it. For example, if your query starts with `op.fromView('demo', 'persons')` and you have an "ID" column, the column
will have a name of "demo.persons.ID".


## Limiting how many rows are returned

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


## Configuring a constraint column

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


## Configuring storage of a constraint column value

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

## Source connector logging

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

## Source connector error handling

Per Kafka's recommendation for error handling for source connectors, the MarkLogic Kafka connector catches and logs any
error that occurs when Kafka polls the connector.

The level of detail in a logged error can be controlled by adjusting [Kafka logging](https://docs.confluent.io/platform/current/connect/logging.html).
The package to configure in Kafka's Log4J logging is `com.marklogic.kafka`. If set to "DEBUG", the error message will
be logged at the "ERROR" level and the error stacktrace will be logged at the "DEBUG" level. Otherwise, the error message
will be logged at the "ERROR" level and the stacktrace will not be logged.

In addition, always check the MarkLogic server log files, particularly those associated with the MarkLogic app server
that the connector connects to, for additional information about the error.

For any errors pertaining to execution of the Optic query run by the connector, it is often helpful to debug these by
running the Optic query via [MarkLogic's qconsole tool](https://docs.marklogic.com/guide/qconsole/intro). You may find
it helpful to simplify the query as much as possible, get it working, and then progressively build the query back up
until it fails. 


