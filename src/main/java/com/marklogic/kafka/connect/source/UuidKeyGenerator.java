package com.marklogic.kafka.connect.source;

import java.util.UUID;

public class UuidKeyGenerator implements KeyGenerator {

    @Override
    public String generateKey() {
        return UUID.randomUUID().toString();
    }
}
