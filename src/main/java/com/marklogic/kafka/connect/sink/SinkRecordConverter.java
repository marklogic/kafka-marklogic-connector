/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Defines how a Kafka SinkRecord is converted into a DocumentWriteOperation, which can then be
 * written to MarkLogic via a WriteBatcher or DocumentManager. Simplifies testing of this logic, as this avoids any
 * dependency on a running MarkLogic or running Kafka instance.
 */
public interface SinkRecordConverter {

    DocumentWriteOperation convert(SinkRecord sinkRecord);

}
