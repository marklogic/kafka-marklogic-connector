# Change Log

## [1.3.0](https://github.com/marklogic-community/kafka-marklogic-connector/releases/tag/1.3.0) (2020-08-18)
   [Full Changelog](https://github.com/marklogic-community/kafka-marklogic-connector/compare/1.3.0...master)

- Support of additional authentication options 
- Documentation of how to update the connector for security options. Refer to [MarkLogic_Kafka_Connector_v1.3.0.pdf](https://github.com/marklogic-community/kafka-marklogic-connector/blob/master/MarkLogic_Kafka_Connector_v1.3.0.pdf) for details.


## [1.2.1](https://github.com/marklogic-community/kafka-marklogic-connector/releases/tag/1.2.1) (2020-05-24)
[Full Changelog](https://github.com/marklogic-community/kafka-marklogic-connector/compare/1.2.1...master)

**Implemented enhancements:**

- Fixed erroneous comments in the connect-standalone properties file.


## [1.2.0](https://github.com/marklogic-community/kafka-marklogic-connector/releases/tag/1.2.0) (2020-04-21)
[Full Changelog](https://github.com/marklogic-community/kafka-marklogic-connector/compare/1.2.0...master)

**Implemented enhancements:**

- Can now run a flow using DHF 5.2.0
- Updated the AWS quickstart document.
- Updated AWS-CloudFormation/cloudFormationTemplates/* with newer AMI, added some steps to CloudFormation-QuickStart.md

## [1.1.0](https://github.com/marklogic-community/kafka-marklogic-connector/releases/tag/v1.1.0) (2019-12-13)
[Full Changelog](https://github.com/marklogic-community/kafka-marklogic-connector/compare/v1.1.0...master)

**Implemented enhancements:**

- Added a feature to permit automatically adding the Kafka topic name as a collection on the documents.
- Documented how to use simple SSL to connect to Kafka brokers and/or MarkLogic clusters.
- Documented how to use multiple instances of the connector in distributed mode.
  
**Fixed bugs:**

- Fixed a bug with handling null, or "tombstone", messages.

## [1.0.0](https://github.com/marklogic-community/kafka-marklogic-connector/releases/tag/v1.0.0) (2019-10-21)
[Full Changelog](https://github.com/marklogic-community/kafka-marklogic-connector/compare/v1.0.0...master)

This version is stable and ready for integration.

**Implemented enhancements:**

- The ability to call a DMSDK transformation on each document.
- Tested with MarkLogic 10.
- Examples of SSL integration with Kafka and MarkLogic.
- The ability to generate a Confluent Connector Archive.

## [0.9.0](https://github.com/marklogic-community/kafka-marklogic-connector/releases/tag/v0.9.0) (2019-08-31)
[Full Changelog](https://github.com/marklogic-community/kafka-marklogic-connector/compare/v0.9.0...master)

- This initial release has all necessary functionality.
- It has been tested extensively in AWS and Docker using test clusters and test data producers.
