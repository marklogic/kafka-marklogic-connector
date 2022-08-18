This guide describes how to develop and contribute pull requests to this connector.

# Testing with Confluent Platform

[Confluent Platform](https://docs.confluent.io/platform/7.2.1/overview.html) provides an easy mechanism for running
Kafka locally via a single application. To try out the MarkLogic Kafka connector via the Confluent Platform, follow
the steps below.

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
3. Click on the "datagen-purchases" connector

Additionally, you can examine the data sent by the Datagen connector to the `purchases` topic:

1. Click on "Topics" in the left sidebar
2. Click on the "purchases" topic
3. Click on "Messages" to see the JSON documents being sent by the connector

## Load a MarkLogic Kafka connector instance

Next, load an instance of the MarkLogic Kafka connector that will read data from the `purchases` topic and write
it to MarkLogic. The `src/test/resources/confluent/marklogic-purchases-connector.json` file defines the connection
properties for MarkLogic, and it defaults to writing to the `Documents` database via port 8000. You can adjust this file
to suit your testing needs.

    ./gradlew loadMarkLogicPurchasesConnector

In the Control Center GUI, you can verify the MarkLogic Kafka connector instance:

1. Click on "Connect" in the left sidebar
2. Click on the "connect-default" cluster
3. Click on the "marklogic-purchases" connector

You can then verify that data is being written to MarkLogic by using MarkLogic's qconsole application to inspect the
contents of the `Documents` database.

You can also manually configure an instance of the MarkLogic Kafka connector as well:

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

## Debugging the MarkLogic Kafka connector

The main mechanism for debugging an instance of the MarkLogic Kafka connector is by examining logs from the 
connector. You can access those, along with logging from Kafka Connect and all other connectors, by running the 
following:

    confluent local services connect log -f

See [the log command docs](https://docs.confluent.io/confluent-cli/current/command-reference/local/services/connect/confluent_local_services_connect_log.html)
for more information.

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

TODO, will borrow a lot of content from the README.
