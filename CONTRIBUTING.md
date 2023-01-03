This guide describes how to develop and contribute pull requests to this connector. The focus is currently on how to
develop and test the connector, either via a local install of Confluent Platform or of the regular Kafka distribution.

Before beginning, you will need to install Java (either version 8, 11, or 17) and also have a MarkLogic instance 
available. It is recommended to use 11 or 17, as Confluent has deprecated Java 8 support in Confluent 7.x and is 
removing it in Confluent 8.x. See [the Confluent compatibility matrix](https://docs.confluent.io/platform/current/installation/versions-interoperability.html#java)
for more information. After installing your desired version of Java, ensure that the `JAVA_HOME` environment variable
points to your Java installation.


# Running the test suite

The test suite for the MarkLogic Kafka connector, found at `src/test/resources`, requires that an application first be
deployed to a MarkLogic instance. This application is deployed via 
[ml-gradle](https://github.com/marklogic-community/ml-gradle). 

Note that you do not need to install [Gradle](https://gradle.org/) - the `gradlew` program used below will install the
appropriate version of Gradle if you do not have it installed already. 

Before deploying, first create `gradle-local.properties`
if it does not yet exist in the root directory of this project and configure `mlPassword` for your `admin` user - e.g.

    mlPassword=changeme

Then deploy the application:

    ./gradlew -i mlDeploy

The application deploys a single REST API app server listening on port 8019; please ensure you have this port available
before deploying.

You can then run the tests via:

    ./gradlew test

Alternatively, you can import this project into an IDE such as IntelliJ and run each of the tests found under 
`src/test/java`.


# Testing with Confluent Platform

[Confluent Platform](https://docs.confluent.io/platform/7.2.1/overview.html) provides an easy mechanism for running
Kafka locally via a single application. A primary benefit of testing with Confluent Platform is to test configuring the
MarkLogic Kafka connector via the [Confluent Control Center](https://docs.confluent.io/platform/current/control-center/index.html) 
web application. 

To try out the MarkLogic Kafka connector via the Confluent Platform, follow the steps below.

## Install Confluent Platform with the MarkLogic Kafka connector

First, [install the Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#cp-quickstart-step-1)
via the "Tar archive" option (the "Docker" option has not yet been tested).

**Important!** After step 6 (installing the Datagen source connector) and before step 7 (starting Confluent Platform), 
you'll need to install the MarkLogic Kafka connector into your Confluent Platform distribution. 

To do so, modify `confluentHome` in `gradle-local.properties` (create this file in the root of this project if it 
does not already exist) to point to where you extracted the Confluent Platform distribution - e.g.:

    confluentHome=/Users/myusername/confluent-7.2.1

Then build and copy the connector to the Confluent Platform directory that you configured above:

    ./gradlew copyConnectorToConfluent

Note that any time you modify the MarkLogic Kafka connector code, you'll need to repeat the 
`./gradlew copyConnectorToConfluent` step.

Next, start Confluent:

    confluent local services start

To verify that your Confluent installation is running properly, you can run `confluent local services status` and
see logging similar to this:

```
Using CONFLUENT_CURRENT: /var/folders/wn/l42pccj17rbfw6h_5b8bt2nnkpch_s/T/confluent.995873
Connect is [UP]
Control Center is [UP]
Kafka is [UP]
Kafka REST is [UP]
ksqlDB Server is [UP]
Schema Registry is [UP]
ZooKeeper is [UP]
```

You can now visit http://localhost:9021 to access [Confluent's Control Center](https://docs.confluent.io/platform/current/control-center/index.html)
application.

Within Control Center, click on "controlcenter.cluster" to access the configuration for the Kafka cluster.


## Load a Datagen connector instance

To test out the MarkLogic Kafka connector, you should first load an instance of the [Kafka Datagen connector]
(https://github.com/confluentinc/kafka-connect-datagen). The Datagen connector is a Kafka source connector that can
generate test data which can then be fed to the MarkLogic Kafka connector. The following Gradle command will automate
loading an instance of the Datagen connector that will write JSON messages to a `purchases` topic every second:

    ./gradlew loadDatagenPurchasesConnector

In the Control Center GUI, you can verify the Datagen connector instance:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on the "datagen-purchases-source" connector

Additionally, you can examine the data sent by the Datagen connector to the `purchases` topic:

1. Click on "Topics" in the left sidebar
2. Click on the "purchases" topic
3. Click on "Messages" to see the JSON documents being sent by the connector

## Load a MarkLogic Kafka sink connector instance

Next, load an instance of the MarkLogic Kafka connector that will read data from the `purchases` topic and write
it to MarkLogic. The `src/test/resources/confluent/marklogic-purchases-sink.json` file defines the connection
properties for MarkLogic. You can adjust this file to suit your testing needs.

    ./gradlew loadMarkLogicPurchasesSinkConnector

In the Control Center GUI, you can verify the MarkLogic Kafka connector instance:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on the "marklogic-purchases-sink" connector

You can then verify that data is being written to MarkLogic by using MarkLogic's qconsole application to inspect the
contents of the `kafka-test-content` database.

You can also manually configure an instance of the sink connector:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on "Add connector"
4. Click on "MarkLogicSinkConnector"
5. Select the topic(s) you wish to read from
6. For "Key converter class" and "Value converter class", enter `org.apache.kafka.connect.storage.StringConverter`
7. Under "General", enter values for the required MarkLogic connection fields and for any optional fields you wish
   to configure
8. At the bottom of the page, click "Next"
9. Click "Launch"

In the list of connectors in Control Center, the connector will initially have a status of "Failed" while it starts up.
After it starts successfully, it will have a status of "Running". 

## Load a MarkLogic Kafka source connector instance

You can also load an instance of the MarkLogic Kafka source connector that will read rows from the `demo/purchases` 
view that is created via the TDE template at `src/test/ml-schemas/tde/purchases.json`. 
The `src/test/reosurces/confluent/marklogic-purchases-source.json` file defines the connection properties for MarkLogic.
You can adjust this file to suit your testing needs.

    ./gradlew loadMarkLogicPurchasesSourceConnector

In the Control Center GUI, you can verify the MarkLogic Kafka connector instance:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on the "marklogic-purchases-source" connector

You can verify that data is being read from the `demo/purchases` view and sent to the `marklogic-purchases` topic 
by clicking on "Topics" in Confluent Platform and then selecting "marklogic-purchases".

You can also manually configure an instance of the source connector:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on "Add connector"
4. Click on "MarkLogicSourceConnector"
5. For "Value converter class", enter `org.apache.kafka.connect.json.JsonConverter`
6. Under "General", enter values for the required MarkLogic connection fields
7. Enter an Optic DSL query for `ml.source.optic.dsl` and a Kafka topic name for `ml.source.topic`
8. Add values for any optional fields that you wish to populate
9. At the bottom of the page, click "Next"
10. Click "Launch"

In the list of connectors in Control Center, the connector will initially have a status of "Failed" while it starts up.
After it starts successfully, it will have a status of "Running".


## Debugging the MarkLogic Kafka connector

The main mechanism for debugging an instance of the MarkLogic Kafka connector is by examining logs from the 
connector. You can access those, along with logging from Kafka Connect and all other connectors, by running the 
following:

    confluent local services connect log -f

See [the log command docs](https://docs.confluent.io/confluent-cli/current/command-reference/local/services/connect/confluent_local_services_connect_log.html)
for more information.

You can also customize Confluent logging by [adjusting the log4j file for Kafka Connect](https://docs.confluent.io/platform/current/connect/logging.html#viewing-kconnect-logs).
For example, to prevent some logging from Kafka Connect and from the Java Client DMSDK, add the following to the 
`$CONFLUENT_HOME/etc/kafka/connect-log4j.properties` file:

    log4j.logger.org.apache.kafka=WARN
    log4j.logger.com.marklogic.client.datamovement=WARN


## Destroying and setting up the Confluent Platform instance

While developing and testing the MarkLogic Kafka connector, it is common that the "local" instance of Confluent 
Platform will become unstable and no longer work. The [Confluent local docs](https://docs.confluent.io/confluent-cli/current/command-reference/local/confluent_local_current.html) 
make reference to this - "The data that are produced are transient and are intended to be temporary". 

It is thus advisable that after you copy a new instance of the MarkLogic Kafka connector into Confluent Platform (i.e. 
by running `./gradlew copyConnectorToConfluent`), you should destroy your local Confluent Platform instance (this 
will usually finish in around 15s):

    ./gradlew destroyLocalConfluent

After doing that, you can quickly automate starting Confluent Platform and loading the two connectors via the 
following (this will usually finish in around 1m):

    ./gradlew setupLocalConfluent

Remember that if you've modified the connector code, you'll first need to run `./gradlew copyConnectorToConfluent`. 

Doing the above will provide the most reliable way to get a new and working instance of Confluent Platform with the 
MarkLogic Kafka connector installed.

For brevity, you may prefer this (Gradle will figure out the tasks as long as only one task starts with "destroy" 
and one task starts with "setup"):

    ./gradlew destroy setup

You may have luck with simply doing `confluent local services stop`, `./gradlew copyConnectorToConfluent`, and 
`confluent local services start`, but this has so far not worked reliably - i.e. one of the Confluent Platform 
services (sometimes Schema Registry, sometimes Control Center) usually stops working. 

# Testing with Apache Kafka

The primary reason to test the MarkLogic Kafka connector via a regular Kafka distribution is that the development 
cycle is much faster and more reliable - i.e. you can repeatedly redeploy the connector and restart Kafka Connect to 
test changes, and Kafka Connect will continue to work fine. This is particularly useful when the changes you're testing
do not require testing the GUI provided by Confluent Control Center.

To get started, these instructions assume that you already have an instance of Apache Kafka installed; the 
[Kafka Quickstart](https://kafka.apache.org/quickstart) instructions provide an easy way of accomplishing this. Perform 
step 1 of these instructions before proceeding.

Next, configure your Gradle properties to point to your Kafka installation and deploy the connector there:

1. Configure `kafkaHome` in gradle-local.properties - e.g. `kafkaHome=/Users/myusername/kafka_2.13-2.8.1`
2. Configure `kafkaMlUsername` and `kafkaMlPassword` in gradle-local.properties, setting these to a MarkLogic user that
   is able to write documents to MarkLogic. These values will be used to populate the 
   `ml.connection.username` and `ml.connection.password` connector properties.
3. Run `./gradlew clean deploy` to build a jar and copy it and the config property files to your Kafka installation

[Step 2 in the Kafka Quickstart guide](https://kafka.apache.org/quickstart) provides the instructions for starting the
separate Zookeeper and Kafka server processes. You'll need to run these commands from your Kafka installation 
directory. As of August 2022, those commands are (these seem very unlikely to change and thus are included here for 
convenience):

    bin/zookeeper-server-start.sh config/zookeeper.properties

and 

    bin/kafka-server-start.sh config/server.properties

Next, start the Kafka connector in standalone mode (also from the Kafka home directory):

    bin/connect-standalone.sh config/marklogic-connect-standalone.properties config/marklogic-sink.properties

You'll see a fair amount of logging from Kafka itself; near the end of the logging, look for messages from
`MarkLogicSinkTask` and MarkLogic Java Client classes such as `WriteBatcherImpl` to ensure that the connector has
started up correctly.

To test out the connector, you can use the following command to enter a CLI that allows you to manually send 
messages to the `marklogic` topic that the connector is configured by default to read from:

    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic marklogic

Be sure that the messages you send are consistent with your configuration properties - i.e. if you've set a format of
JSON, you should send properly formed JSON objects.

When a document is received and written by the connector, you'll see logging like this:

```
[2018-12-20 12:54:13,561] INFO flushing 1 queued docs (com.marklogic.client.datamovement.impl.WriteBatcherImpl:549)
```
