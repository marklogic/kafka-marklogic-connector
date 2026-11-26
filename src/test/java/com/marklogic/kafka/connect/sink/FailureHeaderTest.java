/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.datamovement.impl.WriteEventImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class FailureHeaderTest {

    @Test
    void addsFailureMetadataToOriginalKafkaRecord() {
        ConsumerRecord<byte[], byte[]> original = new ConsumerRecord<>("orders", 2, 17L, null, null);
        WriteEvent event = new WriteEventImpl().withTargetUri("/orders/17.json");

        WriteBatcherSinkTask.addFailureHeadersToOriginalSinkRecord(
            original, new IllegalStateException("write failed"), AbstractSinkTask.MARKLOGIC_WRITE_FAILURE, event);

        Headers headers = original.headers();
        assertHeaderValue(headers, AbstractSinkTask.MARKLOGIC_MESSAGE_FAILURE_HEADER,
            AbstractSinkTask.MARKLOGIC_WRITE_FAILURE);
        assertHeaderValue(headers, AbstractSinkTask.MARKLOGIC_MESSAGE_EXCEPTION_MESSAGE, "write failed");
        assertHeaderValue(headers, AbstractSinkTask.MARKLOGIC_ORIGINAL_TOPIC, "orders");
        assertHeaderValue(headers, AbstractSinkTask.MARKLOGIC_TARGET_URI, "/orders/17.json");
    }

    @Test
    void addsFailureMetadataWithoutOptionalTargetUriOrMessage() {
        ConsumerRecord<byte[], byte[]> original = new ConsumerRecord<>("orders", 2, 17L, null, null);

        WriteBatcherSinkTask.addFailureHeadersToOriginalSinkRecord(
            original, new IllegalStateException(), AbstractSinkTask.MARKLOGIC_CONVERSION_FAILURE, null);

        Headers headers = original.headers();
        assertNull(headers.lastHeader(AbstractSinkTask.MARKLOGIC_TARGET_URI));
        assertNull(headers.lastHeader(AbstractSinkTask.MARKLOGIC_MESSAGE_EXCEPTION_MESSAGE).value());
        assertHeaderValue(headers, AbstractSinkTask.MARKLOGIC_ORIGINAL_TOPIC, "orders");
    }

    private void assertHeaderValue(Headers headers, String key, String expected) {
        Header header = headers.lastHeader(key);
        assertEquals(expected, new String(header.value(), StandardCharsets.UTF_8));
    }
}
