package com.marklogic.client.id.strategy;

import com.marklogic.client.io.marker.AbstractWriteHandle;

import java.util.UUID;


public class DefaultStrategy implements IdStrategy {

    @Override
    public String generateId(AbstractWriteHandle content, String topic, Integer partition, long offset) {
        return UUID.randomUUID().toString();
    }

}
