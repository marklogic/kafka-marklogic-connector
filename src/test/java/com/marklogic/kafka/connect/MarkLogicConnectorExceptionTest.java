/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class MarkLogicConnectorExceptionTest {

    @Test
    void messageOnlyConstructor() {
        MarkLogicConnectorException ex = new MarkLogicConnectorException("something broke");

        assertEquals("something broke", ex.getMessage());
        assertNull(ex.getCause());
    }

    @Test
    void messageAndCauseConstructorRetainsTheCause() {
        IllegalStateException cause = new IllegalStateException("the real problem");

        MarkLogicConnectorException ex = new MarkLogicConnectorException("something broke", cause);

        assertEquals("something broke", ex.getMessage());
        assertSame(cause, ex.getCause(), "The cause must be retained so the root problem reaches the Kafka log");
    }
}
