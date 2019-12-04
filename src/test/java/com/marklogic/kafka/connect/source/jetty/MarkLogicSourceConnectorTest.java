package com.marklogic.kafka.connect.source.jetty;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MarkLogicSourceConnectorTest {

    MarkLogicSourceConnector mlSourceConn;

    @Test
    public void testSourceConnector() throws Exception {
        mlSourceConn = new MarkLogicSourceConnector();
        Map<String, String> props = new HashMap<>();
        props.put(MarkLogicSourceConfig.JETTY_PORT, "9090");
        props.put("topic", "test");
        mlSourceConn.start(props);
        Thread.sleep(1000);
        mlSourceConn.stop();

        assertTrue(true, "Started and stopped the MarkLogic Source Connector");
    }
}
