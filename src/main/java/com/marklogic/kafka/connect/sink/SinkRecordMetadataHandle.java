/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import com.marklogic.client.io.DocumentMetadataHandle;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkRecordMetadataHandle extends DocumentMetadataHandle {

    private SinkRecord sinkRecord;

    SinkRecordMetadataHandle(SinkRecord sinkRecord) {
        super();
        this.sinkRecord = sinkRecord;
    }

    public SinkRecord getSinkRecord() {
        return sinkRecord;
    }
}
