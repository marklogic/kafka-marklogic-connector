/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.source;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RowManagerSourceTaskLifecycleTest {

    @Test
    void stopBeforeStartIsSafeAndRepeatable() {
        ExposedSourceTask task = new ExposedSourceTask();

        assertDoesNotThrow(task::stop, "Kafka can call stop before start ever succeeded");
        assertDoesNotThrow(task::stop, "stop must be idempotent so a second shutdown doesn't fail the worker");
    }

    @Test
    void noConstraintValueIsReturnedWithoutAConstraintValueStore() {
        assertNull(new ExposedSourceTask().previousConstraintValue());
    }

    @Test
    void versionMatchesTheSourceConnectorVersion() {
        assertEquals(MarkLogicSourceConnector.MARKLOGIC_SOURCE_CONNECTOR_VERSION,
            new ExposedSourceTask().version());
    }

    private static class ExposedSourceTask extends RowManagerSourceTask {
        String previousConstraintValue() {
            return getPreviousMaxConstraintColumnValue();
        }
    }
}
