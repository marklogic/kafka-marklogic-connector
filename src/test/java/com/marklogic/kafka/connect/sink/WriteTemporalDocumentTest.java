/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.DocumentDescriptor;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.junit5.XmlNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WriteTemporalDocumentTest extends AbstractIntegrationSinkTest {

    private final static String TEMPORAL_COLLECTION = "kafka-temporal-collection";

    private final static String TEMPORAL_CONTENT = "<tempdoc>" +
        "<content>hello world</content>" +
        "<systemStart>2014-04-03T11:00:00</systemStart>" +
        "<systemEnd>2014-04-03T16:00:00</systemEnd>" +
        "<validStart>2014-04-03T11:00:00</validStart>" +
        "<validEnd>2014-04-03T16:00:00</validEnd>" +
        "</tempdoc>";

    @AfterEach
    void clearDatabase() {
        // Temporal documents must be removed by clearing the database; temporal.documentDelete only "logically" deletes
        // a document, and it otherwise cannot be deleted
        logger.info("Clearing database so that temporal documents are deleted");
        String script = "xdmp:database-forests(xdmp:database()) ! xdmp:forest-clear(.)";
        getDatabaseClient().newServerEval().xquery(script).evalAs(String.class);
    }

    /**
     * This doesn't test the Kafka connector; it's just a sanity check that the test application has a temporal
     * collection configured correctly so that we can expect the Kafka connector to write to a temporal collection
     * correctly.
     */
    @Test
    void verifyThatTestApplicationAllowsForWritingTemporalDocument() {
        final String uri = "/temporal-sanity-check.xml";

        DataMovementManager dmm = getDatabaseClient().newDataMovementManager();
        WriteBatcher writeBatcher = dmm.newWriteBatcher().withTemporalCollection(TEMPORAL_COLLECTION);

        writeBatcher.add(new DocumentWriteOperationImpl(uri,
            new StringHandle(TEMPORAL_CONTENT).withFormat(Format.XML)
        ));

        writeBatcher.flushAndWait();
        dmm.stopJob(writeBatcher);

        DocumentDescriptor doc = getDatabaseClient().newDocumentManager().exists(uri);
        assertNotNull(doc, "Expecting doc to have been written correctly, URI: " + uri);
    }

    @Test
    void kafkaConnectorCanWriteTemporalDocument() {
        AbstractSinkTask task = startSinkTask(
            MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, "kafka1,kafka2",
            MarkLogicSinkConfig.DOCUMENT_TEMPORAL_COLLECTION, TEMPORAL_COLLECTION,
            MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX, ".xml",
            MarkLogicSinkConfig.DOCUMENT_FORMAT, "xml"
        );

        putAndFlushRecords(task, newSinkRecord(TEMPORAL_CONTENT));

        assertCollectionSize(TEMPORAL_COLLECTION, 1);
        // Verify that the normal collections were applied correctly
        assertCollectionSize("kafka1", 1);
        assertCollectionSize("kafka2", 1);

        String temporalDoc = getDatabaseClient().newServerEval()
            .xquery(format("collection('%s')[1]/node()", TEMPORAL_COLLECTION))
            .evalAs(String.class);
        XmlNode doc = new XmlNode(temporalDoc);
        doc.assertElementExists("Verifying that the document was correctly written",
            "/tempdoc/content[. = 'hello world']");
    }

    @Test
    void invalidTemporalCollection() {
        WriteBatcherSinkTask task = (WriteBatcherSinkTask) startSinkTask(
            MarkLogicSinkConfig.DOCUMENT_TEMPORAL_COLLECTION, "not-a-temporal-collection"
        );

        List<String> errorMessages = new ArrayList<>();
        task.getWriteBatcher().onBatchFailure((batch, failure) -> {
            errorMessages.add(failure.getMessage());
        });

        putAndFlushRecords(task, newSinkRecord(TEMPORAL_CONTENT));

        assertCollectionSize(TEMPORAL_COLLECTION, 0);
        assertEquals(1, errorMessages.size(), "An error should have been captured due to the configured " +
            "temporal collection not being an actual temporal collection");
        String message = errorMessages.get(0);
        assertTrue(message.contains("TEMPORAL-COLLECTIONNOTFOUND"), "Unexpected message: " + message);
    }
}
