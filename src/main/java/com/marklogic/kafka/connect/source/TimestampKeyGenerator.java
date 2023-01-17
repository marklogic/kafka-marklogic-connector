package com.marklogic.kafka.connect.source;

/**
 * Generates keys based on a MarkLogic server timestamp. The expectation is that there's value in knowing that a set
 * of records were all retrieved at a particular timestamp, which would allow a user to run a query at that exact
 * timestamp to confirm they get the same results that were sent to Kafka.
 */
public class TimestampKeyGenerator implements KeyGenerator {

    private final long serverTimestamp;
    private int rowNumber;

    public TimestampKeyGenerator(long serverTimestamp) {
        this.serverTimestamp = serverTimestamp;
    }

    @Override
    public String generateKey() {
        return serverTimestamp + "-" + ++rowNumber;
    }
}
