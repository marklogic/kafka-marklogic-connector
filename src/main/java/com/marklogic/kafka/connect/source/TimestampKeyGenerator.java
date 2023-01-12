package com.marklogic.kafka.connect.source;

import java.time.Instant;

public class TimestampKeyGenerator implements KeyGenerator {

    @Override
    public String generateKey(long rowNumber) {
        return Long.toString(Instant.now().toEpochMilli()).concat("-").concat(Long.toString(rowNumber));
    }
}
