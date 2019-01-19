# Configurable Properties for the kafka-connect-marklogic

These are the properties available in marklogic-connect-standalone and marklogic-sink

## marklogic-connect-standalone
This file contains properties that are Kafka-related.
| Property                       | Default Value                                    | Description                                                                                                                                                                   |
|--------------------------------|--------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| bootstrap.servers              | 9092                                             | This points to the Kafka server and port                                                                                                                                      |
| key.converter                  | org.apache.kafka.connect.storage.StringConverter | This controls the format of the data that will be written to Kafka for source connectors or read from Kafka for sink connectors.                                              |
| value.converter                | org.apache.kafka.connect.storage.StringConverter | This controls the format of the data that will be written to Kafka for source connectors or read from Kafka for sink connectors.                                              |
| key.converter.schemas.enable   | false                                            | Control the use of schemas for keys                                                                                                                                           |                                                                                       |
| value.converter.schemas.enable | false                                            | Control the use of schemas for values                                                                                                                                         |                                                                                       |
| offset.storage.file.filename   | /tmp/connect.offsets                             | The file to store connector offsets in. By storing offsets on disk, a standalone process can be stopped and started on a single node and resume where it previously left off. |                                                                                       |
| offset.flush.interval.ms       | 10000                                            | Interval at which to try committing offsets for tasks.                                                                                                                        |                                                                                       |


## marklogic-sink
This file contains properties that are MarkLogic-related.
| Property                       | Default Value                                    | Description                                                                                                                                                                   |
|--------------------------------|--------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
