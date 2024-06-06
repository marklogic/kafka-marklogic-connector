This guide describes how to develop and contribute pull requests to this connector. The focus is currently on how to
develop and test the connector, either via a Docker cluster install of Confluent Platform or of the regular Kafka
distribution.

### Requirements:
* MarkLogic Server 11+
* Java (either version 8, 11, or 17). It is recommended to use 11 or 17, as Confluent has deprecated Java 8 support in
  Confluent 7.x and is removing it in Confluent 8.x. Additionally, Sonar requires the use of Java 11 or 17. 

See [the Confluent compatibility matrix](https://docs.confluent.io/platform/current/installation/versions-interoperability.html#java)
for more information. After installing your desired version of Java, ensure that the `JAVA_HOME` environment variable
points to your Java installation.

# Configuring Local Automated and Manual Testing

The test suite for the MarkLogic Kafka connector, found at `src/test`, requires that the test application first be
deployed to a MarkLogic instance. The recommendation is for this application to be deployed via Docker and
[ml-gradle](https://github.com/marklogic-community/ml-gradle), as described below.

Note that you do not need to install [Gradle](https://gradle.org/) - the "gradlew" program used below will install the
appropriate version of Gradle if you do not have it installed already.

## Virtual Server Preparation
The project includes a docker-compose file that includes MarkLogic, SonarQube with a Postgres server, and Confluent
Platform servers.

### Confluent Platform
[Confluent Platform](https://docs.confluent.io/platform/current/overview.html) provides an easy mechanism for running
Kafka locally via a single Docker cluster. A primary benefit of testing with Confluent Platform is to test configuring
the MarkLogic Kafka connector via the
[Confluent Control Center](https://docs.confluent.io/platform/current/control-center/index.html) web application.
The Confluent Platform servers in this docker-compose file are based on the Confluent files and instructions at
[Install a Confluent Platform cluster in Docker using a Confluent docker-compose file](https://docs.confluent.io/platform/current/platform-quickstart.html).

## Docker Cluster Preparation
To setup the docker cluster, use the docker-compose file in the "test-app" directory to build the Docker cluster with
the command:
```
docker-compose -f docker-compose.yml up -d --build 
```
When the setup is complete, you should be able to run
```
docker-compose -f docker-compose.yml ps
```
and see results similar to the following.
```
NAME                                    IMAGE                                             COMMAND                  SERVICE           CREATED          STATUS          PORTS
broker                                  confluentinc/cp-kafka:7.6.1                       "/etc/confluent/dock…"   broker            14 minutes ago   Up 14 minutes   0.0.0.0:9092->9092/tcp, 0.0.0.0:9101->9101/tcp
connect                                 cnfldemos/cp-server-connect-datagen:0.6.4-7.6.0   "/etc/confluent/dock…"   connect           14 minutes ago   Up 14 minutes   0.0.0.0:8083->8083/tcp, 9092/tcp
control-center                          confluentinc/cp-enterprise-control-center:7.6.1   "/etc/confluent/dock…"   control-center    14 minutes ago   Up 14 minutes   0.0.0.0:9021->9021/tcp
ksql-datagen                            confluentinc/ksqldb-examples:7.6.1                "bash -c 'echo Waiti…"   ksql-datagen      14 minutes ago   Up 14 minutes   
ksqldb-cli                              confluentinc/cp-ksqldb-cli:7.6.1                  "/bin/sh"                ksqldb-cli        14 minutes ago   Up 14 minutes   
ksqldb-server                           confluentinc/cp-ksqldb-server:7.6.1               "/etc/confluent/dock…"   ksqldb-server     14 minutes ago   Up 14 minutes   0.0.0.0:8088->8088/tcp
marklogic                               marklogicdb/marklogic-db:11.2.0-centos-1.1.2      "/tini -- /usr/local…"   marklogic         14 minutes ago   Up 14 minutes   25/tcp, 7997-7999/tcp, 0.0.0.0:8000-8002->8000-8002/tcp, 0.0.0.0:8010-8013->8010-8013/tcp, 8003-8009/tcp, 0.0.0.0:8018-8019->8018-8019/tcp
marklogic-kafka-confluent-postgres-1    postgres:15-alpine                                "docker-entrypoint.s…"   postgres          14 minutes ago   Up 14 minutes   5432/tcp
marklogic-kafka-confluent-sonarqube-1   sonarqube:10.3.0-community                        "/opt/sonarqube/dock…"   sonarqube         14 minutes ago   Up 14 minutes   0.0.0.0:9000->9000/tcp
rest-proxy                              confluentinc/cp-kafka-rest:7.6.1                  "/etc/confluent/dock…"   rest-proxy        14 minutes ago   Up 14 minutes   0.0.0.0:8082->8082/tcp
schema-registry                         confluentinc/cp-schema-registry:7.6.1             "/etc/confluent/dock…"   schema-registry   14 minutes ago   Up 14 minutes   0.0.0.0:8081->8081/tcp
```

You can now visit several web applications:
* http://localhost:8000 to access the MarkLogic server.
* http://localhost:9000 to use the SonarQube server as described in the "Running Sonar Code Analysis"
section below.
* http://localhost:9021 to access
[Confluent's Control Center GUI](https://docs.confluent.io/platform/current/control-center/index.html) application.
Within Control Center, click on "controlcenter.cluster" to access the configuration for the Kafka cluster.

## MarkLogic Preparation
To prepare the MarkLogic server for automated testing as well as testing with the Confluent Platform, the Data Hub based
application must be deployed. From the "test-app" directory, follow these steps:
1. Run `./gradlew hubInit`
2. Edit gradle-local.properties and set `mlUsername` and `mlPassword`
3. Run `./gradlew -i mlDeploy`


## Automated Testing
Now that your MarkLogic server is configured and the test-app is deployed, you can run the tests via from the root
directory:
```
./gradlew test
```
Alternatively, you can import this project into an IDE such as IntelliJ and run each of the tests found under
`src/test/java`.

## Running Sonar Code Analysis

To configure the SonarQube service, perform the following steps:

1. Go to http://localhost:9000 .
2. Login as admin/admin. SonarQube will ask you to change this password; you can choose whatever you want ("password" works).
3. Click on "Create a local project".
4. Enter "marklogic-kafka-connector" for the Project Display Name; use that as the Project Key as well.
5. Enter "master" as the main branch name.
6. Click on "Next".
7. Click on "Use the global setting" and then "Create project".
8. On the "Analysis Method" page, click on "Locally".
9. In the "Provide a token" panel, click on "Generate". Copy the token.
10. Click the "Continue" button.
11. Update `systemProp.sonar.token=<Replace With Your Sonar Token>` in `gradle-local.properties` in the root of your
project.

To run the SonarQube analysis, run the following Gradle task in the root directory, which will run all the tests with
code coverage and then generate a quality report with SonarQube:

    ./gradlew test sonar

If you do not update `systemProp.sonar.token` in your `gradle.properties` file, you can specify the token via the
following:

    ./gradlew test sonar -Dsonar.token=paste your token here

When that completes, you can find the results at http://localhost:9000/dashboard?id=marklogic-kafka-connector

Click on that link. If it's the first time you've run the report, you'll see all issues. If you've run the report
before, then SonarQube will show "New Code" by default. That's handy, as you can use that to quickly see any issues
you've introduced on the feature branch you're working on. You can then click on "Overall Code" to see all issues.

Note that if you only need results on code smells and vulnerabilities, you can repeatedly run "./gradlew sonar"
without having to re-run the tests.

For more assistance with Sonar and Gradle, see the
[Sonar Gradle plugin docs](https://docs.sonarqube.org/latest/analyzing-source-code/scanners/sonarscanner-for-gradle/).


## Confluent Platform for Manual Testing

### Building and Sharing the Connector with the Docker Container
Using gradle in the root directory, build the connector archive and copy it to a directory shared with the Confluent
Platform Docker cluster built in the that section, using this gradle command in the root directory:
```
./gradlew copyConnectorToDockerVolume
```
**You MUST restart the "connect" server in the Docker "confluent-platform-example" cluster.**

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
contents of the `kafka-test-content` database.

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
