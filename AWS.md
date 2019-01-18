# AWS Quick Start Notes

## Create a MarkLogic Instance
* Find "MarkLogic Developer 9" in the AWS Marketplace and subscribe
* Choose a size (I chose R3.2xlarge initially)
* Accepted default VPC & Subnet settings
* Create and choose a Security Group based on MarkLogic's recommended settings
* Create and choose a Key Pair
* Launch

## Create a Kafka Instance
* I chose "Kafka Certified by Bitnami" in the AWS Marketplace and subscribe
* Choose a size (I started with T2.micro)
* Accepted default VPC & Subnet settings
* Choose the Security Group created above
* Choose the Key Pair created above
* Launch

## Starting MarkLogic
* Copy the .pem file to your working directory
* ssh -i kafka.pem <marklogic instance Public DNS>
* vi /etc/marklogic.conf
**    MARKLOGIC_EC2_HOST=0
* service MarkLogic start

## Initializing MarkLogic
* http://<marklogic instance Public DNS>:8001
**  You know how to do this stuff
* Username/Password = admin/admin

## Initializing Kafka
* ssh -i kafka.pem user@<Kafka instance Public DNS>
* Get the Username/Password for Kafka from the EC2 Instance log (not strictly necessary, but you may need it someday) (ybIOJPTahxO0)
* /opt/bitnami/kafka/bin/kafka-topics.sh --create --zookeeper 172.31.81.169:2181 --replication-factor 1 --partitions 1 --topic test
* /opt/bitnami/kafka/bin/kafka-console-producer.sh --broker-list 172.31.81.169:9092 --producer.config /opt/bitnami/kafka/config/producer.properties --topic test
* /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server 172.31.81.169:9092 --topic test --consumer.config /opt/bitnami/kafka/config/consumer.properties --from-beginning

## Producer
/opt/bitnami/kafka/bin/kafka-console-producer.sh --broker-list 172.31.81.169:9092 --producer.config /opt/bitnami/kafka/config/producer.properties --topic test

## Configure & Build the MarkLogic Consumer
* edit marklogic-connect-standalone.properties
** bootstrap.servers=<Kafka instance Public DNS>:9092
* edit marklogic-sink.properties
** topics=test
** ml.connection.host=54.234.68.21
* ./gradlew jar

## Deploy the MarkLogic Consumer
* scp -i kafka.pem config/marklogic-* bitnami@ec2-18-208-156-234.compute-1.amazonaws.com:/tmp
* scp -i kafka.pem build/libs/kafka-connect-marklogic-0.9.0.jar bitnami@ec2-18-208-156-234.compute-1.amazonaws.com:/tmp
* ssh -i kafka.pem bitnami@ec2-18-208-156-234.compute-1.amazonaws.com
** mv /tmp/marklogic-* /opt/bitnami/kafka/config
** chmod 644 /opt/bitnami/kafka/config/marklogic-*
** sudo chown root:root /opt/bitnami/kafka/config/marklogic-*
** mv /tmp/kafka-connect-marklogic-0.9.0.jar /opt/bitnami/kafka/libs
** sudo chmod 644 /opt/bitnami/kafka/libs/kafka-connect-marklogic-0.9.0.jar 
** sudo chown root:root /opt/bitnami/kafka/libs/kafka-connect-marklogic-0.9.0.jar 

## Start the MarkLogic Consumer
sudo /opt/bitnami/kafka/bin/connect-standalone.sh /opt/bitnami/kafka/config/marklogic-connect-standalone.properties /opt/bitnami/kafka/config/marklogic-sink.properties

