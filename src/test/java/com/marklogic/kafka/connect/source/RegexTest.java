package com.marklogic.kafka.connect.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RegexTest {
    @Test
    void testRemovingQuotes() {
        Assertions.assertEquals("remove quotes", QueryHandlerUtil.sanitize("remo've quo\"tes"));
    }
}
