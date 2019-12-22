# Kafka-Connector-MarkLogic AWS Demo and Quick Start
The purpose of this document is to describe how to quickly get a demonstration of this connector running in Amazon Web Services.
This is not intended to be a description of setting up a production environment.

## Create a KeyPair
* In the AWS Web Console, under Network & Security, choose Key Pairs
* Click on Create Key Pair.
* Name the Key Pair and accept the automatic download of the pem file.
* Put the pem file some place convenient.
* Change the permissions on the pem file to restrict access to yourself.
    chmod 600 kafka.pem

## Create a MarkLogic Instance
* Find "MarkLogic Developer 9" in the AWS Marketplace and subscribe (https://aws.amazon.com/marketplace/pp/B072Z536VB)
* Choose a size (I chose R3.2xlarge initially)
* Accepted default VPC & Subnet settings
* Create and choose a Security Group based on MarkLogic's recommended settings
* Create and choose a Key Pair
* Launch

## Create a Kafka Instance
* Find "Kafka Certified by Bitnami" in the AWS Marketplace and subscribe (https://aws.amazon.com/marketplace/pp/B01K0IWPVI)
* Choose a size (I started with T2.micro)
* Accepted default VPC & Subnet settings
* Create a Security Group based on the Bitnami recommendation, and choose that group
* Choose the Key Pair created above
* Launch

## Starting MarkLogic
* ssh -i kafka.pem ec2-user@<marklogic instance Public DNS>
* Turn off the MarkLogic EC2 detection
    sudo su -
    echo "MARKLOGIC_EC2_HOST=0" > /etc/marklogic.conf
* Start MarkLogic
    service MarkLogic start

## Initializing MarkLogic
* http://<marklogic instance Public DNS>:8001
**  You know how to do this stuff
* Username/Password = admin/admin

## Get the Username/Password for Kafka from the EC2 Instance log
#### This is not strictly necessary, but you may need it someday
* In the AWS Web Console, highlight the Kafka instance
* Click Actions -> Instance Settings -> Get System Log
* Find the section that looks like the following
    [   26.303631] bitnami[1160]: #########################################################################
    [   26.318434] bitnami[1160]: #                                                                       #
    [   26.337740] bitnami[1160]: #        Setting Bitnami application password to 'qmVHldrXe3Y6'         #
    [   26.355317] bitnami[1160]: #        (the default application username is 'user')                   #
    [   26.372323] bitnami[1160]: #                                                                       #
    [   26.388808] bitnami[1160]: #########################################################################
* See https://docs.bitnami.com/aws/faq/#how-to-find-application-credentials for more information

## Initializing Kafka
* SSH to the Kafka instance
<pre>ssh -i kafka.pem bitnami@&lt;Kafka instance Public DNS&gt;</pre>
* Turn off SASL authentication (on lines 28, 36, and 142, change “SASL_PLAINTEXT” to “PLAINTEXT”):
<pre>sudo vi /opt/bitnami/kafka/conf/server.properties</pre>
* Restart the Kafka service
<pre>sudo /opt/bitnami/ctlscript.sh restart kafka</pre>
* Create a topic called, "marklogic". (Note - this won't work until the Kafka service has restarted)
<pre>/opt/bitnami/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic marklogic</pre>

## Test Kafka
* In one terminal, /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic marklogic --consumer.config /opt/bitnami/kafka/config/consumer.properties --from-beginning
* In a second terminal, /opt/bitnami/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --producer.config /opt/bitnami/kafka/config/producer.properties --topic marklogic
* In the producer console, enter a message. For example,
    { "Foo" : "bar" }
* The terminal with the consumer should spit out the message you entered in the producer

## Configure & Build the MarkLogic Consumer
* edit marklogic-connect-standalone.properties
    bootstrap.servers=<Kafka instance Public DNS>:9092
* edit marklogic-sink.properties
    topics=marklogic
    ml.connection.host=<marklogic instance Public DNS>
* Build the jar file
    ./gradlew jar

## Deploy the MarkLogic Consumer
* scp -i kafka.pem config/marklogic-* bitnami@<Kafka instance Public DNS>:/tmp
* scp -i kafka.pem build/libs/kafka-connect-marklogic-<version>.jar bitnami@<Kafka instance Public DNS>:/tmp
* ssh -i kafka.pem bitnami@<Kafka instance Public DNS>
    sudo mv /tmp/marklogic-* /opt/bitnami/kafka/config
    sudo chmod 644 /opt/bitnami/kafka/config/marklogic-*
    sudo chown root:root /opt/bitnami/kafka/config/marklogic-*
    sudo mv /tmp/kafka-connect-marklogic-<version>.jar /opt/bitnami/kafka/libs
    sudo chmod 644 /opt/bitnami/kafka/libs/kafka-connect-marklogic-<version>.jar 
    sudo chown root:root /opt/bitnami/kafka/libs/kafka-connect-marklogic-<version>.jar 

## Start the MarkLogic Consumer
* While still connected to the Kafka server
    sudo /opt/bitnami/kafka/bin/connect-standalone.sh /opt/bitnami/kafka/config/marklogic-connect-standalone.properties /opt/bitnami/kafka/config/marklogic-sink.properties
* This takes a few seconds to initialize.
* After loading is complete, it will process the message you entered earlier
* Use QConsole (http://<marklogic instance Public DNS>:8000) to verify the message was ingested in the database
* Assuming you did not change "ml.document.uriPrefix" in marklogic-sink.properties, the URI will be of the following form:
    /kafka-data/{UUID}.json
