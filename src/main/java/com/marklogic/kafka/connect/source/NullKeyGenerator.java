package com.marklogic.kafka.connect.source;

public class NullKeyGenerator implements KeyGenerator {

    @Override
    public String generateKey(long rowNumber) {
        return null;
    }
}
