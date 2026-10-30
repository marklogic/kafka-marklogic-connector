/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies the lifecycle paths that Kafka Connect drives on a started source task.
 */
class SourceTaskLifecycleTest extends AbstractIntegrationSourceTest {

    @Test
    void pollReturnsNullInsteadOfFailingTheTaskWhenTheQueryIsInvalid() throws InterruptedException {
        RowManagerSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, "this is not a valid Optic DSL query",
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

        List<SourceRecord> records = task.poll();

        assertNull(records, "A failed query must be logged and return null, otherwise Kafka fails the whole task");
        task.stop();
    }

    @Test
    void pollReturnsNullWhenTheQueryMatchesNoRows() throws InterruptedException {
        RowManagerSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL + ".where(op.eq(op.col('ID'), -1))",
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

         ch.qos.logback.classic.Logger taskLogger = (ch.qos.logback.classic.Logger)
             org.slf4j.LoggerFactory.getLogger(RowManagerSourceTask.class);
         ch.qos.logback.classic.Level originalLevel = taskLogger.getLevel();
         ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
             new ch.qos.logback.core.read.ListAppender<>();
         appender.start();
         taskLogger.addAppender(appender);
         taskLogger.setLevel(ch.qos.logback.classic.Level.ERROR);
         try {
             assertNull(task.poll(), "Kafka prefers null over an empty list when there is no data");
             assertNull(appender.list.stream()
                 .filter(event -> event.getFormattedMessage().startsWith("Unable to poll for source records;"))
                 .findFirst().orElse(null), "The query must succeed, not return null because polling failed");
         } finally {
             taskLogger.detachAppender(appender);
             appender.stop();
             taskLogger.setLevel(originalLevel);
             task.stop();
         }
    }

    @Test
    void stopReleasesTheDatabaseClientOfAStartedTask() {
        RowManagerSourceTask task = startSourceTask(
            MarkLogicSourceConfig.DSL_QUERY, AUTHORS_OPTIC_DSL,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC
        );

        assertDoesNotThrow(task::stop);
    }
}
