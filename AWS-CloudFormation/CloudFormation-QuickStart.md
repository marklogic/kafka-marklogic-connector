# Quick Start with an AWS CloudFormation Template
The purpose of this document is to describe how to quickly build a set of three servers to demonstrate the kafka-marklogic-connector.
The three servers are the Kafka/Zookeeper server, the MarkLogic server, and a server to run the connector and to produce messages.

_This is not intended to be a description of setting up a production environment._

_All the resource files have been configure and compiled specifically for this example.
In particular, IP addresses are in the config files._

## Requirements
* You just need to have an AWS account.

## Create a Key Pair in AWS
1. Navigate to your [AWS Key Pairs](https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#KeyPairs:sort=keyName)
1. Click "Create Key Pair".
1. Name the Key Pair, 'kafka', and click "Create".
1. When prompted, save the PEM file locally.

## Build the AWS Resources
1. Navigate to the [AWS CloudFormation](https://console.aws.amazon.com/cloudformation/home?region=us-east-1) page.
1. Click "Create Stack"
1. Click the "Upload a template file" radio button
1. Click "Upload File" and upload "readyToGo.json"
1. Click "Next"
1. Give your stack a name, and click "Next"
1. Click "Next"
1. Click "Create Stack"
1. Go to the list of your [AWS Instances](https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:sort=tag:Name)
1. Now wait until initialization is complete.

## Start the Connector
1. On the list of you AWS Instances, click on the instance named, "TemplateBased-Kafka-Worker-A".
1. Copy the Public DNS (IPv4).
1. Ssh to the TemplateBased-Kafka-Worker-A server.
`ssh -i kafka.pem bitnami@<Public DNS>`
1. Start the connector
`sudo /opt/bitnami/kafka/bin/connect-standalone.sh /opt/bitnami/kafka/config/marklogic-connect-standalone.properties /opt/bitnami/kafka/config/marklogic-sink.properties`

## Generate some Messages
This step uses the JAR file from a small project for producing test messages. It can be found in [my GitHub account](https://github.com/BillFarber/KafkaProducer)
1. On the list of you AWS Instances, click on the instance named, "TemplateBased-Kafka-Worker-A".
1. Copy the Public DNS (IPv4).
1. Ssh to the TemplateBased-Kafka-Worker-A server.
`ssh -i kafka.pem bitnami@<Public DNS>`
1. Send some messages to the Kafka topic
`java -jar /home/bitnami/kafka-producer-1.0-SNAPSHOT.jar -c 4 -m 5 -h ip-172-31-48-44.ec2.internal:9092 -t marklogic`
