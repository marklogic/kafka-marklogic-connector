package com.marklogic.kafka.connect.source;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;

public class RecordContent {

    AbstractWriteHandle content;
    DocumentMetadataHandle additionalMetadata;
    String id;

    public AbstractWriteHandle getContent() {
        return content;
    }

    public void setContent(AbstractWriteHandle content) {
        this.content = content;
    }

    public DocumentMetadataHandle getAdditionalMetadata() {
        return additionalMetadata;
    }

    public void setAdditionalMetadata(DocumentMetadataHandle additionalMetadata) {
        this.additionalMetadata = additionalMetadata;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}

