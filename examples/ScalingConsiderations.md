# Scaling Considerations
Each of the three parts of parts (Kafka, MarkLogic, and this connector) of this system maybe easily scaled to handle
your throughput requirements. To use the connector in a clustered environment you only need to ensure a couple of
properties are set correctly. Kafka and MarkLogic handle most of the details for you.

## Kafka
Kafka brokers in a cluster communicate with each other and relay status. When a connector is started and connects to
a broker, this status information is used to tell the connector what brokers to communicate with. When brokers are
started or shutdown, this information is also relayed to the connectors so that they can be updated.

## MarkLogic
MarkLogic is designed to be used in large clusters of servers. In order to spread the load of data I/O across the
cluster, a load balancer is typically used. In this case, the connector should be configured to be aware of the use
of a load balancer. This is accomplished by setting the "ml.connection.host" to point to the load balancer, and by setting "ml.connection.type" to "gateway" in the marklogic-sink.properties
file.

<pre><code># A MarkLogic host to connect to. The connector uses the Data Movement SDK, and thus it will connect to each of the
# hosts in a cluster.
ml.connection.host=MarkLogic-LoadBalancer-1024238516.us-east-1.elb.amazonaws.com

# Optional - set to "gateway" when using a load balancer, else leave blank.
# See https://docs.marklogic.com/guide/java/data-movement#id_26583 for more information.
ml.connection.type=gateway</code></pre>

## Connector
When configuring multiple instances of the connector to consume the same topic(s), the Kafka Connect framework
automatically handles dividing up the connections by assigning specific topic partitions (spread across the Kafka
cluster) to specific connector instances. The framework does this based on the 'group.id' setting in
marklogic-connect-standalone.properties. So, if a new connector is started with the same ‘group.id’ as a connector that
is already running,the partitions will get re-assigned across all the matching connectors. Conversely, if a connector
shuts down, the partitions are re-assigned across the remaining partitions.
