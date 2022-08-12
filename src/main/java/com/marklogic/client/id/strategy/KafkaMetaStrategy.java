package com.marklogic.client.id.strategy;

import com.marklogic.client.io.marker.AbstractWriteHandle;

public class KafkaMetaStrategy implements IdStrategy {

    @Override
    public String generateId(AbstractWriteHandle content, String topic, Integer partition, long offset) {
        String id = "";
        id = topic + "/" + partition.toString() + "/" + String.valueOf(offset);
        return id;
    }

}
