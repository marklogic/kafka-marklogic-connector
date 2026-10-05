/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.source;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ConstraintValueStoreTest {

    @Test
    void createsNoStoreWithoutConstraintColumn() {
        assertNull(ConstraintValueStore.newConstraintValueStore(null, new HashMap<>()));
    }

    @Test
    void createsInMemoryStoreWhenStorageUriIsNotConfigured() {
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "ID");

        ConstraintValueStore store = ConstraintValueStore.newConstraintValueStore(null, config);

        assertInstanceOf(InMemoryConstraintValueStore.class, store);
        assertNull(store.retrievePreviousMaxConstraintColumnValue());
        store.storeConstraintState("42", 3);
        assertEquals("42", store.retrievePreviousMaxConstraintColumnValue());
        store.storeConstraintState(null, 0);
        assertNull(store.retrievePreviousMaxConstraintColumnValue());
    }

    @Test
    void treatsBlankConstraintOptionsAsNotConfigured() {
        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, "  ");
        config.put(MarkLogicSourceConfig.CONSTRAINT_STORAGE_URI, "/state.json");

        assertNull(ConstraintValueStore.newConstraintValueStore(null, config));
    }
}
