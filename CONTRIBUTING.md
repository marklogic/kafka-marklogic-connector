This guide describes how to develop and contribute pull requests to this connector. The focus is currently on how to
develop and test the connector, either via a Docker cluster install of Confluent Platform or of the regular Kafka distribution.

Before beginning, you will need to install Java (either version 8, 11, or 17) and also have a MarkLogic instance
available. It is recommended to use 11 or 17, as Confluent has deprecated Java 8 support in Confluent 7.x and is
removing it in Confluent 8.x. Additionally, Sonar requires the use of Java 11 or 17.
See [the Confluent compatibility matrix](https://docs.confluent.io/platform/current/installation/versions-interoperability.html#java)
for more information. After installing your desired version of Java, ensure that the `JAVA_HOME` environment variable
points to your Java installation.


# Running the test suite

The test suite for the MarkLogic Kafka connector, found at `src/test/resources`, requires that an application first be
deployed to a MarkLogic instance. This application is deployed via Docker and [ml-gradle](https://github.com/marklogic-community/ml-gradle).

Note that you do not need to install [Gradle](https://gradle.org/) - the `gradlew` program used below will install the
appropriate version of Gradle if you do not have it installed already. 

Then deploy the application to a MarkLogic Docker container built using a docker-compose file:

    docker-compose up -d --build
    <Wait 20 to 30 seconds and verify that <http://localhost:8001> shows the MarkLogic admin screen before proceeding.>
    ./gradlew -i mlDeploy

The application deploys a single REST API app server listening on port 8019; please ensure you have this port available
before deploying.

You can then run the tests via:

    ./gradlew test

Alternatively, you can import this project into an IDE such as IntelliJ and run each of the tests found under 
`src/test/java`.

# Running Sonar code analysis

In order to use SonarQube, you must have used Docker to run this project's `docker-compose.yml` file, and you must
have those services running.

To configure the SonarQube service, perform the following steps:

1. Go to http://localhost:9000 .
2. Login as admin/admin. SonarQube will ask you to change this password; you can choose whatever you want ("password" works).
3. Click on "Create project manually".
4. Enter "marklogic-kafka-connector" for the Project Display Name; use that as the Project Key as well.
5. Enter "master" as the main branch name.
6. Click on "Next".
8. Click on "Use the global setting" and then "Create project".
8. On the "Analysis Method" page, click on "Locally".
9. In the "Provide a token" panel, click on "Generate". Copy the token.
10. Update `systemProp.sonar.token=<Replace With Your Sonar Token>` in `gradle.properties` in the root of your project.

To run SonarQube, run the following Gradle tasks, which will run all the tests with code coverage and then generate
a quality report with SonarQube:

    ./gradlew test sonar

If you do not update `systemProp.sonar.token` in your `gradle.properties` file, you can specify the token via the
following:

    ./gradlew test sonar -Dsonar.token=paste your token here

When that completes, you can find the results at http://localhost:9000/dashboard?id=marklogic-kafka-connector

Click on that link. If it's the first time you've run the report, you'll see all issues. If you've run the report
before, then SonarQube will show "New Code" by default. That's handy, as you can use that to quickly see any issues
you've introduced on the feature branch you're working on. You can then click on "Overall Code" to see all issues.

Note that if you only need results on code smells and vulnerabilities, you can repeatedly run `./gradlew sonar`
without having to re-run the tests.

For more assistance with Sonar and Gradle, see the [Sonar Gradle plugin docs](https://docs.sonarqube.org/latest/analyzing-source-code/scanners/sonarscanner-for-gradle/).


# Testing with Confluent Platform

[Confluent Platform](https://docs.confluent.io/platform/current/overview.html) provides an easy mechanism for running
Kafka locally via a single application. A primary benefit of testing with Confluent Platform is to test configuring the
MarkLogic Kafka connector via the [Confluent Control Center](https://docs.confluent.io/platform/current/control-center/index.html) 
web application. 

To try out the MarkLogic Kafka connector via the Confluent Platform, follow the steps below.

## Build a Confluent Platform cluster with the MarkLogic and the Kafka connector

### Build the Confluent Platform cluster via Docker

**Note** - This installs a separate Docker cluster and is not the same as the Docker cluster described in the test
section at the top of this document.

Use the docker-compose file in "src/test/confluent-platform-example" to build the Confluent Platform Docker cluster
with the command ```docker-compose -f src/test/confluent-platform-example/docker-compose.yml up -d --build```.
This file is based on the Confluent files and instructions at
[Install a Confluent Platform cluster in Docker using a Confluent docker-compose file]
(https://docs.confluent.io/platform/current/platform-quickstart.html).
When the setup is complete, you should be able to run
```docker-compose -f src/test/confluent-platform-example/docker-compose.yml ps``` and see the following results.
```
    Name                    Command               State                               Ports
--------------------------------------------------------------------------------------------------------------
broker            /etc/confluent/docker/run        Up      0.0.0.0:9092->9092/tcp,:::9092->9092/tcp,
0.0.0.0:9101->9101/tcp,:::9101->9101/tcp
connect           /etc/confluent/docker/run        Up      0.0.0.0:8083->8083/tcp,:::8083->8083/tcp, 9092/tcp
control-center    /etc/confluent/docker/run        Up      0.0.0.0:9021->9021/tcp,:::9021->9021/tcp
ksql-datagen      bash -c echo Waiting for K ...   Up
ksqldb-cli        /bin/sh                          Up
ksqldb-server     /etc/confluent/docker/run        Up      0.0.0.0:8088->8088/tcp,:::8088->8088/tcp
rest-proxy        /etc/confluent/docker/run        Up      0.0.0.0:8082->8082/tcp,:::8082->8082/tcp
schema-registry   /etc/confluent/docker/run        Up      0.0.0.0:8081->8081/tcp,:::8081->8081/tcp
```

You can now visit http://localhost:9021 to access [Confluent's Control Center](https://docs.confluent.io/platform/current/control-center/index.html) application.

Within Control Center, click on "controlcenter.cluster" to access the configuration for the Kafka cluster.

### Build and install the MarkLogic Kafka Connector
1. Build the connectorArchive target using ```./gradlew installConnectorInConfluent```.
2. Restart the "connect" server in the Docker "confluent-platform-example" cluster.
3. Verify the connector has loaded properly.
   1. Click on "Connect" in the left sidebar.
   2. Click on the "connect-default" cluster.
   3. Click on the "+ Add connector" tile.
   4. The "Browse" screen should several tiles including "MarkLogicSinkConnector" and "MarkLogicSourceConnector".


### Install the test application on the MarkLogic server in the Docker cluster
In the project root directory, run ```./gradlew -i mlDeploy```

## Load a Datagen connector instance

### Via Gradle
```./gradlew -i loadDatagenPurchasesConnector```

### Via curl
```curl -X POST -H "Content-Type: application/json" --data @src/test/resources/confluent/datagen-purchases-source.json http://localhost:8083/connectors```

### Verifying the new connector instance
In the Control Center GUI, you can verify the Datagen connector instance:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on the "datagen-purchases-source" connector

Additionally, you can examine the data sent by the Datagen connector to the `purchases` topic:

1. Click on "Topics" in the left sidebar
2. Click on the "purchases" topic
3. Click on "Messages" to see the JSON documents being sent by the connector

## Load a MarkLogic Kafka sink connector instance

### Via Gradle
```./gradlew -i loadMarkLogicPurchasesSinkConnector```

### Via curl
```curl -X POST -H "Content-Type: application/json" --data @src/test/resources/confluent/marklogic-purchases-sink.json http://localhost:8083/connectors```

### Verifying the new connector instance
In the Control Center GUI, you can verify the MarkLogic Kafka connector instance:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on the "marklogic-purchases-sink" connector

You can then verify that data is being written to MarkLogic by using MarkLogic's qconsole application to inspect the
contents of the `kafka-test-content` database.

### Via the web application
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

### Via Gradle
```./gradlew -i loadMarkLogicPurchasesSourceConnector```

### Via curl
```curl -X POST -H "Content-Type: application/json" --data @src/test/resources/confluent/marklogic-purchases-source.json http://localhost:8083/connectors```

### Verifying the new connector instance
In the Control Center GUI, you can verify the MarkLogic Kafka connector instance:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on the "marklogic-purchases-source" connector

You can verify that data is being read from the `demo/purchases` view and sent to the `marklogic-purchases` topic 
by clicking on "Topics" in Confluent Platform and then selecting "marklogic-purchases".

### Via the web application
You can also manually configure an instance of the source connector:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on "Add connector"
4. Click on "MarkLogicSourceConnector"
5. For "Value converter class", enter `org.apache.kafka.connect.storage.StringConverter`
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
by running `./gradlew installConnectorInConfluent`), you should destroy your local Confluent Platform instance (this 
will usually finish in around 15s):

    ./gradlew destroyLocalConfluent

After doing that, you can quickly automate starting Confluent Platform and loading the two connectors via the 
following (this will usually finish in around 1m):

    ./gradlew setupLocalConfluent

Remember that if you've modified the connector code, you'll first need to run `./gradlew installConnectorInConfluent`. 

Doing the above will provide the most reliable way to get a new and working instance of Confluent Platform with the 
MarkLogic Kafka connector installed.

For brevity, you may prefer this (Gradle will figure out the tasks as long as only one task starts with "destroy" 
and one task starts with "setup"):

    ./gradlew destroy setup

You may have luck with simply doing `confluent local services stop`, `./gradlew installConnectorInConfluent`, and 
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

Next, start the Kafka connector in standalone mode (also from the Kafka home directory). To run the sink connector, 
use the following command:

    bin/connect-standalone.sh config/marklogic-connect-standalone.properties config/marklogic-sink.properties

To run the source connector, the command is:

    bin/connect-standalone.sh config/marklogic-connect-standalone.properties config/marklogic-source.properties

You'll see a fair amount of logging from Kafka itself; near the end of the logging, look for messages from
`MarkLogicSinkTask` or `MarkLogicSourceTask` and MarkLogic Java Client classes such as `WriteBatcherImpl` or
`RowManagerSourceTask` to ensure that the connector has started up correctly.

## Sink Connector Testing
To test out the sink connector, you can use the following command to enter a CLI that allows you to manually send 
messages to the `marklogic` topic that the connector is configured by default to read from:

    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic marklogic

Be sure that the messages you send are consistent with your configuration properties - i.e. if you've set a format of
JSON, you should send properly formed JSON objects.

When a document is received and written by the connector, you'll see logging like this:

```
[2018-12-20 12:54:13,561] INFO flushing 1 queued docs (com.marklogic.client.datamovement.impl.WriteBatcherImpl:549)
```

## Source Connector Testing
To test out the source connector, you can insert documents into MarkLogic that contain data that matches TDE templates
to generate Optic rows that match your query. For example, if your TDE template defines an "Authors" view in the
"Medical" schema, the source configuration file might have the following properties.

    ml.source.optic.dsl=op.fromView("Medical", "Authors")
    ml.source.topic=Authors

Then, after the connector is running, monitor the "Authors" topic. When a document is retrieved from MarkLogic and
delivered by the connector to the topic, you'll see logging like this:

    [2023-01-10 09:58:23,326] INFO [marklogic-source|task-0] DSL query: op.fromView("Medical", "Authors") (com.marklogic.kafka.connect.source.DslQueryHandler:37)
    [2023-01-10 09:58:23,395] INFO [marklogic-source|task-0] Source record count: 3; duration: 66 (com.marklogic.kafka.connect.source.RowManagerSourceTask:79)

You can monitor the number of records in the target Topic using a tool included with Kafka in the bin directory:

    bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic Authors

You can also use a Kafka tool to pull messages from the Topic. If you run this command while loading records from MarkLogic,
it will monitor the topic and output the records as they are delivered:

    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic Authors
