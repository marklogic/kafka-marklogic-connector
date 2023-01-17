package com.marklogic.kafka.connect.source;

public class NullKeyGenerator implements KeyGenerator {

    @Override
    public String generateKey() {
        return null;
    }
}
