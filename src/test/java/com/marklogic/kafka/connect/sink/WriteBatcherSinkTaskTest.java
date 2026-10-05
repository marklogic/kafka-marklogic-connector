/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class WriteBatcherSinkTaskTest {

    @Test
    void flushAndStopAreSafeBeforeTheTaskIsStarted() {
        WriteBatcherSinkTask task = new WriteBatcherSinkTask();

        assertDoesNotThrow(() -> task.flush(null),
            "Kafka can call flush before start succeeded, so the WriteBatcher may not exist yet");
        assertDoesNotThrow(task::stop);
    }
}
