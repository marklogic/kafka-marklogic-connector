package com.marklogic.kafka.connect.sink.idstrategy;

import com.marklogic.client.io.marker.AbstractWriteHandle;

import java.util.UUID;

public interface IdStrategy {
    default String generateId(AbstractWriteHandle content, String topic, Integer partition, long offset) {
        return UUID.randomUUID().toString();
    }
}
