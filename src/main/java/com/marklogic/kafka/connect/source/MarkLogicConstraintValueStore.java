package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.document.DocumentDescriptor;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import org.springframework.util.StringUtils;

import java.util.Map;

public class MarkLogicConstraintValueStore extends ConstraintValueStore {
    private final DefaultDocumentPermissionsParser permissionsParser = new DefaultDocumentPermissionsParser();
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
        } catch (JsonProcessingException e) {
            String errorMessage = "Unable to store constraint value (" + constraintStateJson + ") at URI: "
                + constraintStorageUri + "; cause: " + e.getMessage();
            throw new RuntimeException(errorMessage, e);
        } catch (FailedRequestException e) {
            String errorMessage = "Unable to store constraint value (" + constraintStateJson + ") at URI: "
                + constraintStorageUri + "; cause: " + e.getMessage();
            throw new RuntimeException(errorMessage, e);
        }
    }

    private void insertPermissions(DocumentMetadataHandle metadataHandle) {
        if (StringUtils.hasText(constraintStoragePermissions)) {
            permissionsParser.parsePermissions(constraintStoragePermissions, metadataHandle.getPermissions());
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
