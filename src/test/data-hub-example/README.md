This DHF project is intended for testing out the DHF integration in the sink connector. To install this, do the 
following:

1. Run `./gradlew hubInit`
2. Edit gradle-local.properties and set `mlUsername` and `mlPassword`
3. Run `./gradlew mlDeploy`

The DHF integration has been tested successfully with a plain Kafka distribution, but we've run into errors with 
Confluent Platform due to classpath issues that have not yet been resolved. See the ./CONTRIBUTING.md guide on 
testing with plain Kafka. You'll need to modify the following properties in ./config/marklogic-sink.properties:

1. ml.datahub.flow.name=example1
2. ml.datahub.flow.steps=1
3. ml.datahub.flow.logResponse=true
4. ml.connection.port=8010

Any data you send to the "marklogic" topic (as defined in marklogic-sink.properties) will then be written to the 
DHF staging database via port 8010. The above flow will then be run, causing the document to be wrapped in an 
envelope and written to the DHF final database. 
