package com.marklogic.kafka.connect.source.jetty;

import com.marklogic.kafka.connect.source.MessageQueue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MarklogicJettySourceTaskTest {

    MessageQueue queue = MessageQueue.getInstance();
    SourceTask task = new MarkLogicJettySourceTask();

    @BeforeEach
    public void setup() {
        queue.clear();
        queue.enqueue("first", "plain/text", "first");
        queue.enqueue("second", "application/json", "{ \"foo\":\"bar\" }");
        queue.enqueue(new MessageQueue.Message("third", "text/xml", "<foo>bar</foo>"));
        task.start(new HashMap<>());
    }

    @Test
    public void testPoll() throws InterruptedException {
        List<SourceRecord> records = task.poll();
        assertEquals(3, records.size());

        SourceRecord record = records.get(0);
        assertEquals("first", record.topic());
        assertEquals("first", record.value().toString());

        record = records.get(1);
        assertEquals("second", record.topic());
        assertEquals("{ \"foo\":\"bar\" }", record.value().toString());

        record = records.get(2);
        assertEquals("third", record.topic());
        assertEquals("<foo>bar</foo>", record.value().toString());

        // make sure queue was cleaned up
        assertEquals(0, queue.size());
        records = task.poll();
        assertEquals(0, records.size());
    }

    @AfterEach
    public void teardown() {
        task.stop();
    }
}
