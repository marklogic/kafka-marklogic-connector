/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink.idstrategy;

import com.marklogic.client.io.marker.AbstractWriteHandle;

public class KafkaMetaStrategy implements IdStrategy {

    @Override
    public String generateId(AbstractWriteHandle content, String topic, Integer partition, long offset) {
        return topic + "/" + partition.toString() + "/" + offset;
    }

}
