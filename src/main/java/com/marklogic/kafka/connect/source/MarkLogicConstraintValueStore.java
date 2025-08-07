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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentDescriptor;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.kafka.connect.MarkLogicConnectorException;
import org.springframework.util.StringUtils;

import java.util.Map;

public class MarkLogicConstraintValueStore extends ConstraintValueStore {
    private final DatabaseClient databaseClient;
    private final String constraintStorageUri;
    private String constraintStoragePermissions;
    private String constraintStorageCollections;

    MarkLogicConstraintValueStore(DatabaseClient databaseClient, String constraintStorageUri, String constraintColumn,
            Map<String, Object> parsedConfig) {
        super(constraintColumn);
        this.databaseClient = databaseClient;
        this.constraintStorageUri = constraintStorageUri;
        constraintStoragePermissions = (String) parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_STORAGE_PERMISSIONS);
        if (!StringUtils.hasText(constraintStoragePermissions)) {
            constraintStoragePermissions = null;
        }
        constraintStorageCollections = (String) parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_STORAGE_COLLECTIONS);
        if (!StringUtils.hasText(constraintStorageCollections)) {
            constraintStorageCollections = null;
        }
    }

    public void storeConstraintState(String previousMaxConstraintColumnValue, int lastRowCount) {
        String constraintStateJson = "";
        try {
            constraintStateJson = buildConstraintState(previousMaxConstraintColumnValue, lastRowCount);
            StringHandle handle = new StringHandle(constraintStateJson).withFormat(Format.JSON);
            DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
            insertPermissions(metadataHandle);
            insertCollections(metadataHandle.getCollections());
            JSONDocumentManager mgr = databaseClient.newJSONDocumentManager();
            mgr.write(constraintStorageUri, metadataHandle, handle);
        } catch (Exception e) {
            String errorMessage = String.format("Unable to store constraint document %s at URI: %s; cause: %s",
                    constraintStateJson, constraintStorageUri, e.getMessage());
            throw new MarkLogicConnectorException(errorMessage, e);
        }
    }

    private void insertPermissions(DocumentMetadataHandle metadataHandle) {
        if (StringUtils.hasText(constraintStoragePermissions)) {
            metadataHandle.getPermissions().addFromDelimitedString(constraintStoragePermissions);
        }
    }

    private void insertCollections(DocumentMetadataHandle.DocumentCollections documentCollections) {
        if (StringUtils.hasText(constraintStorageCollections)) {
            documentCollections.addAll(constraintStorageCollections.split(","));
        }
    }

    public String retrievePreviousMaxConstraintColumnValue() {
        JacksonHandle resultHandle = new JacksonHandle();
        DocumentDescriptor descriptor = databaseClient.newJSONDocumentManager().exists(constraintStorageUri);
        if (descriptor != null) {
            databaseClient.newJSONDocumentManager().read(constraintStorageUri, resultHandle);
            return resultHandle.get().findPath("marklogicKafkaConstraintLastValue").textValue();
        } else {
            logger.info("Did not find constraint value document at URI: {}", constraintStorageUri);
            return null;
        }
    }
}
