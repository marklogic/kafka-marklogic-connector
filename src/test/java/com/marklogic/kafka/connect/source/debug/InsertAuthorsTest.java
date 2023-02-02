/*
 * Copyright (c) 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.kafka.connect.source.debug;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.junit5.spring.SimpleTestConfig;
import com.marklogic.kafka.connect.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Convenience program for inserting a new author into the kafka-test-content database to facilitate testing the
 * source connector on the authors TDE. This reuses the test plumbing so we don't have to collect host/port/etc
 * information, but it adjusts the DatabaseClient config to use port 8018 instead of 8019 so that documents are
 * inserted into the kafka-test-content database instead of kafka-test-test-content.
 */
class InsertAuthorsTest extends AbstractIntegrationTest {

    // Only intended for inserting new authors with a user-provided ID. Can easily expand this to make the data more
    // random and/or controlled by the user.
    private final static String CITATIONS_DOC = "<Citations>\n" +
        "  <Citation>\n" +
        "    <ID>%%AUTHOR_ID%%</ID>\n" +
        "    <Article>\n" +
        "      <Journal>\n" +
        "        <ISSN>61296-004</ISSN>\n" +
        "        <JournalIssue>\n" +
        "          <Volume>44</Volume>\n" +
        "          <PubDate>\n" +
        "            <Date>2022-07-13</Date>\n" +
        "            <Year>2022</Year>\n" +
        "            <Month>07</Month>\n" +
        "            <Day>13</Day>\n" +
        "            <Time>09:00:00</Time>\n" +
        "          </PubDate>\n" +
        "        </JournalIssue>\n" +
        "      </Journal>\n" +
        "      <AuthorList>\n" +
        "        <Author>\n" +
        "          <LastName>LastName%%AUTHOR_ID%%</LastName>\n" +
        "          <ForeName>ForeName%%AUTHOR_ID%%</ForeName>\n" +
        "        </Author>\n" +
        "      </AuthorList>\n" +
        "    </Article>\n" +
        "  </Citation>\n" +
        "</Citations>\n";

    @Autowired
    SimpleTestConfig testConfig;

    @Test
    @SuppressWarnings("java:S2699") // No assertions needed for this
    void insertAuthors() {
        final String authorIds = System.getProperty("AUTHOR_IDS");
        if (authorIds == null || authorIds.trim().length() < 1) {
            System.out.println("Please specify AUTHOR_IDS as a system property containing a comma-delimited list of author IDs; \n" +
                "if running this via Gradle, use -PauthorId=someNumber to specify the value");
            return;
        }

        DatabaseClientConfig config = testConfig.databaseClientConfig();
        config.setPort(8018);
        DatabaseClient client = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(config);

        XMLDocumentManager mgr = client.newXMLDocumentManager();
        DocumentWriteSet writeSet = mgr.newWriteSet();
        List<String> uris = new ArrayList<>();
        DocumentMetadataHandle metadata = new DocumentMetadataHandle()
            .withPermission("kafka-test-minimal-user", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE);

        for (String authorId : authorIds.split(",")) {
            final String uri = "/citation/author/" + authorId + ".xml";
            uris.add(uri);
            String xml = CITATIONS_DOC.replaceAll("%%AUTHOR_ID%%", authorId);
            writeSet.add(uri, metadata, new StringHandle(xml).withFormat(Format.XML));
        }
        mgr.write(writeSet);
        System.out.println("Successfully inserted new authors with URIs: " + uris);
    }
}
