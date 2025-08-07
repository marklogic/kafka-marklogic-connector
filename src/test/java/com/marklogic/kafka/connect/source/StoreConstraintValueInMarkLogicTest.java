/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
package com.marklogic.kafka.connect.source;

import com.marklogic.client.MarkLogicIOException;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.junit5.PermissionsTester;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StoreConstraintValueInMarkLogicTest extends AbstractIntegrationSourceTest {

    private final String constraintColumnName = "ID";
    private final String limitedAuthorsDsl = AUTHORS_OPTIC_DSL + ".orderBy(op.asc(op.col('" + constraintColumnName
            + "')))";
    private final String constraintStorageUri = "/kafka/currentConstrainInfo";
    private final String roleName = "kafka-test-minimal-user";
    private final String constraintStoragePermissions = roleName + ",read," + roleName + ",update";
    private final String constraintStorageCollections = "kafka,maxValue";
    private final String[] taskConfigStrings = new String[] {
            MarkLogicSourceConfig.DSL_QUERY, limitedAuthorsDsl,
            MarkLogicSourceConfig.TOPIC, AUTHORS_TOPIC,
            MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME, constraintColumnName,
            MarkLogicSourceConfig.CONSTRAINT_STORAGE_URI, constraintStorageUri,
            MarkLogicSourceConfig.CONSTRAINT_STORAGE_PERMISSIONS, constraintStoragePermissions,
            MarkLogicSourceConfig.CONSTRAINT_STORAGE_COLLECTIONS, constraintStorageCollections
    };
    private final Map<String, Object> parsedConfig = new HashMap<String, Object>() {
        {
            for (int i = 0; i < taskConfigStrings.length / 2; i++) {
                put(taskConfigStrings[i * 2], taskConfigStrings[(i * 2) + 1]);
            }
        }
    };
    ConstraintValueStore constraintValueStore;
    RowManagerSourceTask task;

    @BeforeEach
    void givenFifteenAuthors() {
        loadFifteenAuthorsIntoMarkLogic();
        constraintValueStore = new MarkLogicConstraintValueStore(getDatabaseClient(), constraintStorageUri,
                constraintColumnName, parsedConfig);
        task = startSourceTask(taskConfigStrings);
    }

    @Test
    void saveConstraintValueAndUseForNextCall() throws InterruptedException, IOException {
        List<SourceRecord> newRecords = task.poll();
        assertEquals(15, newRecords.size(), "All initial records should be returned");
        assertConstraintStateDocumentPermissionsCollectionsAndValue(task.getPreviousMaxConstraintColumnValue());

        givenNewRowsWithIdsBeforeAndAfterTheCurrentMaxValue();
        newRecords = task.poll();
        assertOnlyRowsWithIdsAfterTheCurrentMaxValueGetDelivered(newRecords);

        String newLastValue = constraintValueStore.retrievePreviousMaxConstraintColumnValue();
        assertEquals(task.getPreviousMaxConstraintColumnValue(), newLastValue,
                "The stored lastValue should have been updated to the value from the second run of the task");

        String evenLaterTime = "03:01:00";
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("thirdHigherId", "8", evenLaterTime, "thirdHigherId");
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("fourthHigherId", "9", evenLaterTime, "fourthHigherId");

        storeBadConstraintState();
        Assertions.assertThrows(
                MarkLogicIOException.class,
                () -> constraintValueStore.retrievePreviousMaxConstraintColumnValue(),
                "Attempting to retrieve the Max Constraint Value should fail");

        Assertions.assertNull(task.poll(), "Since the stored state document is invalid, poll() should fail");
    }

    @Test
    void pollAfterWritingInvalidConstraintState() throws InterruptedException {
        constraintValueStore.storeConstraintState("invalid-value", 0);

        List<SourceRecord> newRecords = task.poll();

        assertNull(newRecords, "No records are returned because 'invalid-value' causes Optic to crash.");
    }

    @Test
    void writeConstraintStateDocumentWithoutPermissions() throws InterruptedException {
        List<SourceRecord> newRecords = task.poll();
        assertEquals(15, newRecords.size(), "All initial records should be returned");

        givenConstraintStoreWithoutPermissions();

        Assertions.assertNull(task.poll(),
                "Attempting to retrieve the Max Constraint Value should fail, causing poll() to fail");
    }

    private void assertConstraintStateDocumentPermissionsCollectionsAndValue(String previousMaxConstraintColumnValue) {
        String lastValue = constraintValueStore.retrievePreviousMaxConstraintColumnValue();
        assertEquals(previousMaxConstraintColumnValue, lastValue,
                "The stored lastValue should equal the value from the Task");
        PermissionsTester permTester = this.readDocumentPermissions(constraintStorageUri);
        permTester.assertReadPermissionExists(roleName);
        permTester.assertUpdatePermissionExists(roleName);
        this.assertInCollections(constraintStorageUri, constraintStorageCollections.split(","));
    }

    private void givenNewRowsWithIdsBeforeAndAfterTheCurrentMaxValue() throws IOException {
        String laterTime = "02:01:00";
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("firstHigherId", "6", laterTime, "firstHigherId");
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("secondHigherId", "7", laterTime, "secondHigherId");
        loadSingleAuthorRowIntoMarkLogicWithCustomTime("lowestId", "0", laterTime, "lowestId");
    }

    private void assertOnlyRowsWithIdsAfterTheCurrentMaxValueGetDelivered(List<SourceRecord> newRecords) {
        assertEquals(2, newRecords.size(), "Only 2 of the 3 new records should be returned");
        assertTrue(newRecords.toString().contains("\"Medical.Authors.ID\":6"),
                newRecords + " did not contain the expected value");
        assertTrue(newRecords.toString().contains("\"Medical.Authors.ID\":7"),
                newRecords + " did not contain the expected value");
    }

    private void storeBadConstraintState() {
        String badConstraintState = "<Not>StateData</Not>";
        XMLDocumentManager XMLDocMgr = getDatabaseClient().newXMLDocumentManager();
        StringHandle handle = new StringHandle(badConstraintState);
        XMLDocMgr.write(constraintStorageUri, handle);
    }

    private void givenConstraintStoreWithoutPermissions() {
        DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
        metadataHandle.getPermissions().addFromDelimitedString("admin,read,admin,update");
        String badConstraintState = "{\"A\": \"a\"}";
        JSONDocumentManager mgr = getDatabaseClient().newJSONDocumentManager();
        StringHandle handle = new StringHandle(badConstraintState);
        mgr.write(constraintStorageUri, metadataHandle, handle);
    }
}
