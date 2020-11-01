# Quick Start with an AWS CloudFormation Template
The purpose of this document is to describe how to quickly build a set of three servers to demonstrate the kafka-marklogic-connector.
The three servers are the Kafka/Zookeeper server, the MarkLogic server, and a server to run the connector and to produce messages.

_This is not intended to be a description of setting up a production environment._

_All the resource files have been configured and compiled specifically for this example.
In particular, IP addresses are in the config files._

## Requirements
* You need to have an AWS account
* You need to have subscribed to the [MarkLogic AMI](https://aws.amazon.com/marketplace/pp/B072Z536VB?ref_=aws-mp-console-subscription-detail)
* You need to have subscribed to the [Kafka Certified by Bitnami AMI](https://aws.amazon.com/marketplace/pp/B01K0IWPVI?qid=1587125714910&sr=0-1&ref_=srh_res_product_title)

## Create a Key Pair in AWS
Please note: if you are using the default values in the resource files, you will need to be in the us-east-1 (Virginia) region when you follow these steps.

1. Navigate to your [AWS Key Pairs](https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#KeyPairs:sort=keyName). 

1. Click "Create Key Pair".

1. Name the Key Pair, 'kafka', and click "Create".

1. When prompted, save the PEM file locally.

1. Change the file permissions on the PEM file:

   ```
   chmod 400 kafka.pem
   ```

## Build the AWS Resources
1. Navigate to the [AWS CloudFormation](https://console.aws.amazon.com/cloudformation/home?region=us-east-1) page.
1. Click "Create Stack" and "With new resources (standard)"
1. Click the "Upload a template file" radio button
1. Click "Upload File" and upload "readyToGo.json"
1. Click "Next"
1. Give your stack a name, and click "Next"
1. Click "Next"
1. Click "Create Stack"
1. Go to the list of your [AWS Instances](https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:sort=tag:Name)
1. <strong>Now wait until initialization is complete.<strong> (Instance Status Check says "2/2 checks passed")

## View the empty Kafka database in MarkLogic

1. On the list of your AWS instances, click on the instance named "instanceMarkLogicA".
2. Copy the Public DNS (IPv4)
3. In a browser, open the page http://\<public DNS>:8000
   * Enter the username "admin"
   * Enter the password "admin"
   * (If this page does not open for you, wait 5 minutes to allow server initialization to complete)
4. In the QConsole UI, change the *Database* to "Kafka"
5. Click *Explore* to verify that the database has no documents.

## Start the Connector
1. On the list of your AWS Instances, click on the instance named, "TemplateBased-Kafka-Worker-A".
1. Copy the Public DNS (IPv4).
1. Ssh to the TemplateBased-Kafka-Worker-A server.
```
ssh -i kafka.pem bitnami@<Public DNS>
```
1. Start the connector
```
sudo /opt/bitnami/kafka/bin/connect-standalone.sh /opt/bitnami/kafka/config/marklogic-connect-standalone.properties /opt/bitnami/kafka/config/marklogic-sink.properties
```

## Generate some Messages
This step uses the JAR file from a small project for producing test messages. It can be found in [my GitHub account](https://github.com/BillFarber/KafkaProducer)
1. On the list of your AWS Instances, click on the instance named, "TemplateBased-Kafka-Worker-A".
1. Copy the Public DNS (IPv4).
1. Ssh to the TemplateBased-Kafka-Worker-A server.
```
ssh -i kafka.pem bitnami@<Public DNS>
```
1. Send some messages to the Kafka topic
```
java -jar /home/bitnami/kafka-producer-1.0-SNAPSHOT.jar -c 4 -m 5 -h ip-172-31-48-44.ec2.internal:9092 -t marklogic
```

## View the Messages in MarkLogic

1. Go back to the MarkLogic QConsole web page (see previous instructions)
1. Click *Explore* to view the documents you created from "Kafka-Worker-A"
