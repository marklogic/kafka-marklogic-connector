---
layout: default
title: Configuring the Connector
nav_order: 4
---

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

## Overview

Using the MarkLogic Kafka connector requires configuring a set of properties to control how the connector interacts
with MarkLogic. The manner in which you use the MarkLogic connector will determine how you configure the connector:

- If you are using the connector via Confluent Control Center, the Control Center GUI will list all the connector
  properties with inline help showing a description of each property.
- If you are using the connector by running Kafka Connect from the command line, you'll be configuring a copy of the
  `./config/marklogic-sink.properties` file or `./config/marklogic-source.properties` file in this repository which
  lists all the connector properties with a description for each one.

When using either the sink or source connector, you'll need to configure two Kafka properties that control
[how data is serialized](https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/)
when it is sent to the MarkLogic connector. These two properties along with their recommended values are:

- `key.converter=org.apache.kafka.connect.storage.StringConverter`
- `value.converter=org.apache.kafka.connect.storage.StringConverter`

Again, the manner in which you use the MarkLogic connector will determine how you configure the above two properties:

- If you are using the connector via Confluent Control Center, the Control Center GUI will allow you to configure these
  two properties in the "Common" section
- If you are using the connector by running Kafka Connect from the command line, you'll configure these properties in a
  copy of the `./config/marklogic-connect-standalone.properties` file. This file already has the two properties set to the
  correct values, so you should not need to do anything further with them.

## Configuring the connection

You must configure the connector to control how it connects to MarkLogic. The properties you must configure are determined
by the type of authentication required by the MarkLogic app server that you connect to and whether it requires SSL usage.

Regardless of the required authentication strategy, you must configure the following properties:

- `ml.connection.host` = the name of a host in the MarkLogic cluster you wish to connect to
- `ml.connection.port` = the port of the MarkLogic app server you wish to connect to
- `ml.connection.securityContextType` = the authentication strategy required by the MarkLogic app server; defaults to DIGEST

The choices for `ml.connection.securityContextType` are DIGEST, BASIC, CERTIFICATE, KERBEROS, and NONE. The additional
properties required for each are described in the following sections.

### Configuring digest and basic authentication

Both digest and basic authentication require the following properties to be configured:

- `ml.connection.username` = the name of the MarkLogic user to authenticate as
- `ml.connection.password` = the password of the MarkLogic user

### Configuring MarkLogic Cloud authentication

Cloud authentication requires the following properties to be configured:

- `ml.connection.basePath` = the base path in your MarkLogic Cloud instance that points to the REST API server you
  wish to connect to
- `ml.connection.cloudApiKey` = the API key for authenticating with your MarkLogic Cloud instance

You should also set `ml.connection.port` to 443 for connecting to MarkLogic Cloud.

### Configuring certificate authentication

Certificate authentication requires the following properties to be configured:

- `ml.connection.certFile` = path to a PKCS12 certificate file
- `ml.connection.certPassword` = password for the PKCS12 certificate file

### Configuring Kerberos authentication

Kerberos authentication requires the following property to be configured:

- `ml.connection.externalName` = the name of the principal to be used in Kerberos authentication

### Configuring no authentication

A value of "NONE" is used when the MarkLogic app server uses "application-level" authentication. No additional
properties need to be required for this authentication strategy.

### Configuring SSL usage

The MarkLogic connector provides two options for configuring the usage of SSL when connecting to MarkLogic:

- `ml.connection.simpleSsl` = `true` to use a "trust-everything" approach; this is useful during development for easier testing
- `ml.connection.enableCustomSsl` = `true` to utilize other properties for a custom SSL strategy

If a custom SSL approach is used, you can use the following properties to configure this approach:

- `ml.connection.customSsl.tlsVersion` = the TLS version to use for constructing an `SSLContext`. Defaults to `TLS`, permitting the JVM to use the highest version possible.
- `ml.connection.customSsl.mutualAuth` = `true` to configure mutual, or "2-way", SSL authentication

If `ml.connection.customSsl.mutualAuth` is set to `true`, you must also configure these properties:

- `ml.connection.certFile` = path to a PKCS12 key store
- `ml.connection.certPassword` = password for the PKCS12 key store


