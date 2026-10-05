/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class BulkDataServicesSinkTaskTest {

    @Test
    void flushAndStopAreSafeBeforeTheTaskIsStarted() {
        BulkDataServicesSinkTask task = new BulkDataServicesSinkTask();

        assertDoesNotThrow(() -> task.flush(null),
            "Kafka can call flush before start succeeded, so the BulkInputCaller may not exist yet");
        assertDoesNotThrow(task::stop);
    }
}
