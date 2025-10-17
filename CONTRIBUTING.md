This guide describes how to develop and contribute pull requests to this connector. The focus is currently on how to
develop and test the connector. There are two methods available - automated and manual. Both methods are performed via a
Docker stack. The automated tests stack creates a MarkLogic instance for the automated tests. The
manual tests use Confluent Platform in a different Docker stack to allow testing the connector via Confluent Control
Center with a MarkLogic instance in the same stack.

### Requirements:
* MarkLogic Server 11+
* Java version 17

See [the Confluent compatibility matrix](https://docs.confluent.io/platform/current/installation/versions-interoperability.html#java)
for more information. After installing your desired version of Java, ensure that the `JAVA_HOME` environment variable
points to your Java installation.

# Configuring Local Automated Testing

The test suite for the MarkLogic Kafka connector, found at `src/test`, requires that the test application first be
deployed to a MarkLogic instance. The recommendation is for this application to be deployed via Docker and
[ml-gradle](https://github.com/marklogic-community/ml-gradle), as described below.

Note that you do not need to install [Gradle](https://gradle.org/) - the "gradlew" program used below will install the
appropriate version of Gradle if you do not have it installed already.

## Docker Cluster Preparation for Automated Testing
The automated tests require a MarkLogic server. The docker-compose file in the repository root includes these services.
To prepare for running the automated tests, perform the following steps:
```
docker-compose up -d --build 
```

You can now visit this web applications:
* http://localhost:8000 to access the MarkLogic server.

## MarkLogic Preparation
To prepare the MarkLogic server for automated testing as well as testing with the Confluent Platform, the Data Hub based
application must be deployed. From the root directory, follow these steps:
1. Run `./gradlew hubInit`
3. Run `./gradlew -i mlDeploy`

Note: If you change the version of Data Hub Framework used by this project, you should also delete the following directories:
* 'test-app/src/main/entity-config'
* 'test-app/src/main/hub-internal-config'


## Automated Testing
Now that your MarkLogic server is configured and the test-app is deployed, you can run the tests via from the root
directory. Note that you must be using Java 17 for this command due to the latest version of Gradle.
```
./gradlew test
```
Alternatively, you can import this project into an IDE such as IntelliJ and run each of the tests found under
`src/test/java`.

## Generating code quality reports with SonarQube

Please see our [internal Wiki page](https://progresssoftware.atlassian.net/wiki/spaces/PM/pages/1763541097/Developer+Experience+SonarQube)
for information on setting up SonarQube if you have not yet already.


# Configuring Local Manual Testing
This project includes a Docker Compose file that creates a Kafka cluster using Confluent Platform along with a
MarkLogic server. This allows you to test the MarkLogic Kafka connector via the Confluent Control Center web
application. The instructions below describe how to get started.

## Docker Cluster Preparation for Manual Testing
The docker-compose file in the test-app directory includes these services along with a MarkLogic server.
```
docker-compose --env-file test-app/.env -f test-app/docker-compose.yml up -d --build
```

When the setup is complete, you should be able to run
```
docker-compose --env-file test-app/.env -f test-app/docker-compose.yml ps
```
and see results similar to the following.
```
NAME                                                 IMAGE                                                                                                      COMMAND                  SERVICE             CREATED          STATUS          PORTS
alertmanager                                         confluentinc/cp-enterprise-alertmanager:2.2.0                                                              "alertmanager-start"     alertmanager        51 seconds ago   Up 50 seconds   0.0.0.0:9093->9093/tcp, [::]:9093->9093/tcp
broker                                               confluentinc/cp-server:8.0.0                                                                               "/etc/confluent/dock…"   broker              51 seconds ago   Up 50 seconds   0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp, 0.0.0.0:9101->9101/tcp, [::]:9101->9101/tcp
connect                                              cnfldemos/cp-server-connect-datagen:0.6.7-8.0.0                                                            "/etc/confluent/dock…"   connect             51 seconds ago   Up 49 seconds   0.0.0.0:8083->8083/tcp, [::]:8083->8083/tcp
control-center                                       confluentinc/cp-enterprise-control-center-next-gen:2.2.0                                                   "/etc/confluent/dock…"   control-center      51 seconds ago   Up 49 seconds   0.0.0.0:9021->9021/tcp, [::]:9021->9021/tcp
flink-jobmanager                                     cnfldemos/flink-kafka:1.19.1-scala_2.12-java17                                                             "/docker-entrypoint.…"   flink-jobmanager    51 seconds ago   Up 50 seconds   0.0.0.0:9081->9081/tcp, [::]:9081->9081/tcp
flink-sql-client                                     cnfldemos/flink-sql-client-kafka:1.19.1-scala_2.12-java17                                                  "/docker-entrypoint.…"   flink-sql-client    51 seconds ago   Up 50 seconds   6123/tcp, 8081/tcp
flink-taskmanager                                    cnfldemos/flink-kafka:1.19.1-scala_2.12-java17                                                             "/docker-entrypoint.…"   flink-taskmanager   51 seconds ago   Up 50 seconds   6123/tcp, 8081/tcp
ksql-datagen                                         confluentinc/ksqldb-examples:8.0.0                                                                         "bash -c 'echo Waiti…"   ksql-datagen        51 seconds ago   Up 49 seconds   
ksqldb-cli                                           confluentinc/cp-ksqldb-cli:8.0.0                                                                           "/bin/sh"                ksqldb-cli          51 seconds ago   Up 49 seconds   
ksqldb-server                                        confluentinc/cp-ksqldb-server:8.0.0                                                                        "/etc/confluent/dock…"   ksqldb-server       51 seconds ago   Up 49 seconds   0.0.0.0:8088->8088/tcp, [::]:8088->8088/tcp
manual-tests-marklogic-kafka-confluent-marklogic-1   ml-docker-db-dev-tierpoint.bed-artifactory.bedford.progress.com/marklogic/marklogic-server-ubi:latest-12   "/tini -- /usr/local…"   marklogic           51 seconds ago   Up 50 seconds   0.0.0.0:8000-8002->8000-8002/tcp, [::]:8000-8002->8000-8002/tcp, 0.0.0.0:8010-8013->8010-8013/tcp, [::]:8010-8013->8010-8013/tcp, 0.0.0.0:8018-8019->8018-8019/tcp, [::]:8018-8019->8018-8019/tcp
prometheus                                           confluentinc/cp-enterprise-prometheus:2.2.0                                                                "prometheus-start"       prometheus          51 seconds ago   Up 50 seconds   0.0.0.0:9090->9090/tcp, [::]:9090->9090/tcp
rest-proxy                                           confluentinc/cp-kafka-rest:8.0.0                                                                           "/etc/confluent/dock…"   rest-proxy          51 seconds ago   Up 49 seconds   0.0.0.0:8082->8082/tcp, [::]:8082->8082/tcp
schema-registry                                      confluentinc/cp-schema-registry:8.0.0                                                                      "/etc/confluent/dock…"   schema-registry     51 seconds ago   Up 50 seconds   0.0.0.0:8081->8081/tcp, [::]:8081->8081/tcp
```

You can now visit several web applications:
* http://localhost:8000 to access the MarkLogic server.
* http://localhost:9021 to access
  [Confluent's Control Center GUI](https://docs.confluent.io/platform/current/control-center/index.html) application.
  Within Control Center, click on "controlcenter.cluster" to access the configuration for the Kafka cluster.

### Confluent Platform for Manual Testing
[Confluent Platform](https://docs.confluent.io/platform/current/overview.html) provides an easy mechanism for running
Kafka locally via a single Docker cluster. A primary benefit of testing with Confluent Platform is to test configuring
the MarkLogic Kafka connector via the
[Confluent Control Center](https://docs.confluent.io/platform/current/control-center/index.html) web application.
The Confluent Platform servers in this docker-compose file are based on the Confluent files and instructions at
[Install a Confluent Platform cluster in Docker using a Confluent docker-compose file](https://docs.confluent.io/platform/current/platform-quickstart.html).


### MarkLogic Preparation
Please ensure you've followed the instructions for "MarkLogic Preparation" in the "Configuring Local Automated Testing"
sectuib above for deploying a Data Hub test application.

Note: If you change the version of Data Hub Framework used by this project, you should also delete the following directories:
* 'test-app/src/main/entity-config'
* 'test-app/src/main/hub-internal-config'


### Building and Sharing the Connector with the Docker Container
Using gradle in the root directory, build the connector archive and copy it to a directory shared with the Confluent
Platform Docker cluster built in the that section, using this gradle command in the root directory:
```
./gradlew copyConnectorToDockerVolume
```
**You MUST restart the "connect" server in the Docker "manual-tests-marklogic-kafka-confluent" cluster.**

Now, verify the connector has loaded properly.
1. Click on "Connect" in the left sidebar.
2. Click on the "connect-default" cluster.
3. Click on the "+ Add connector" tile.
4. The "Browse" screen should several tiles including "MarkLogicSinkConnector" and "MarkLogicSourceConnector".

### Load a Datagen connector instance
In order to create an instance of the Datagen connector and start generating some messages, run this command from the
"test-app" directory.
```
./gradlew -i loadDatagenPurchasesConnector
```

#### Verifying the new connector instance
In the Control Center GUI, you can verify the Datagen connector instance:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on the "datagen-purchases-source" connector

Additionally, you can examine the data sent by the Datagen connector to the `purchases` topic:

1. Click on "Topics" in the left sidebar
2. Click on the "purchases" topic
3. Click on "Messages" to see the JSON documents being sent by the connector

### Load a MarkLogic Kafka sink connector instance
Also, in the "test-app" directory, run:
```
./gradlew -i loadMarkLogicPurchasesSinkConnector
```

#### Verifying the new connector instance
In the Control Center GUI, you can verify the MarkLogic Kafka connector instance:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on the "marklogic-purchases-sink" connector

You can then verify that data is being written to MarkLogic by using MarkLogic's qconsole application to inspect the
contents of the `data-hub-FINAL` database. There should be documents with URIs that start with `/purchase/*`.

### Load a MarkLogic Kafka source connector instance
You can also load an instance of the MarkLogic Kafka source connector that will read rows from the `demo/purchases`
view that is created via the TDE template at `src/test/ml-schemas/tde/purchases.json`.
```
./gradlew -i loadMarkLogicPurchasesSourceConnector
```

#### Verifying the new connector instance
In the Control Center GUI, you can verify the MarkLogic Kafka connector instance:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on the "marklogic-purchases-source" connector

You can verify that data is being read from the `demo/purchases` view and sent to the `marklogic-purchases` topic
by clicking on "Topics" in Confluent Platform and then selecting "marklogic-purchases".


## Data Hub Testing

### Load a MarkLogic Kafka Data Hub sink connector instance
Also in the "test-app" directory, run:
```
./gradlew -i loadMarkLogicDHPurchasesSinkConnector
```

#### Verifying the new connector instance
In the Control Center GUI, you can verify the MarkLogic Kafka connector instance:

1. Click on `Connect` in the left sidebar
2. Click on the `connect-default` cluster
3. Click on the `marklogic-DH-purchases-sink` connector

You can then verify that data is being written to MarkLogic by using MarkLogic's qconsole application to inspect the
contents of the `data-hub-FINAL` database.


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


# Testing with basic Apache Kafka
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

## Testing the Docs service

User documents designed to be published with GitHub Pages and are maintained in the /docs directory of the
project. You must have Ruby installed. Additionally, there seems to be a bug with running jekyll with Ruby 3.3.
The server needs to be run with Ruby 3.2.3, so you will need to run `chruby ruby-3.2.3` before starting the jekyll
server. To start the jekyll server, cd into the /docs directory and run the command `bundle exec jekyll server`.
This will start the server and the user documents will be available at http://127.0.0.1:4000/.

# Publishing the Connector to Confluent

Please refer to the internal Wiki page for information regarding the process for releasing the connector to the Confluent Hub.
