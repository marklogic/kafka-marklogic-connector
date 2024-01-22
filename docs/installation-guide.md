---
layout: default
title: Installation Guide
nav_order: 3
---

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

## Overview

Apache Kafka can be run either as a standalone product or via a provider such as Confluent. This guide provides
instructions on using the MarkLogic connector with either Kafka as a standalone product or via Confluent. Other
Kafka providers exist and should be capable of utilizing the MarkLogic connector as well, but instructions on those
are out of scope of this document.

## Using the connector with Confluent Platform

[Confluent Platform](https://docs.confluent.io/platform/current/platform.html) provides an easy mechanism for running
Kafka via a single application, which includes a simple process for installing new connectors.

The MarkLogic Kafka connector can be installed via the instructions at
[Confluent Hub](https://www.confluent.io/hub/marklogic/kafka-marklogic-connector). After installing it, you can use
[Confluent Control Center](https://docs.confluent.io/platform/current/control-center/index.html) to load and configure
as many instances of the MarkLogic Kafka connector that you wish.

## Using the connector with Apache Kafka

For a regular installation of Apache Kafka, obtain the latest version of the MarkLogic Kafka connector from
[this repository's Releases page](https://github.com/marklogic/kafka-marklogic-connector/releases). Download
the jar file - named `kafka-connect-marklogic-(version).jar` - and copy it to the `./libs` directory in your Kafka
distribution.

Next, copy the `config/marklogic-connect-standalone.properties`, `config/marklogic-sink.properties` and/or
`config/marklogic-source.properties` files in this repository to the `./config` directory in your Kafka distribution.
These three files are required when running Kafka Connect for the following purposes:

1. `marklogic-connect-standalone.properties` defines several properties required by Kafka Connect
2. `marklogic-sink.properties` defines properties specific to the MarkLogic Kafka sink connector (include this file when
   retrieving records from a Kafka topic and sending them to MarkLogic)
3. `marklogic-source.properties` defines properties specific to the MarkLogic Kafka source connector (include this file
   when retrieving records from MarkLogic and submitting them to a Kafka topic)

You can then start up a [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) process. Kafka
Connect will instantiate the MarkLogic Kafka connector based on the configuration in the files above. 


