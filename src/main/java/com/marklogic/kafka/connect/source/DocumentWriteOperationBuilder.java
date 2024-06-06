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
package com.marklogic.kafka.connect.source;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.springframework.util.StringUtils;

public class DocumentWriteOperationBuilder {

    private DocumentWriteOperation.OperationType operationType = DocumentWriteOperation.OperationType.DOCUMENT_WRITE;
    private String uriPrefix;
    private String uriSuffix;
    private String collections;
    private String permissions;

    public DocumentWriteOperation build(RecordContent recordContent) {

        AbstractWriteHandle content = recordContent.getContent();
        DocumentMetadataHandle metadata = recordContent.getAdditionalMetadata();
        String uri = recordContent.getId();

        if (content == null) {
            throw new NullPointerException("'content' must not be null");
        }
        if (StringUtils.hasText(collections)) {
            metadata.getCollections().addAll(collections.trim().split(","));
        }
        if (StringUtils.hasText(permissions)) {
            metadata.getPermissions().addFromDelimitedString(permissions.trim());
        }

        if (StringUtils.hasText(uriPrefix)) {
            uri = uriPrefix + uri;
        }
        if (StringUtils.hasText(uriSuffix)) {
            uri += uriSuffix;
        }

        return build(operationType, uri, metadata, content);
    }

    /**
     * Exists to give a subclass a chance to further customize the contents of a DocumentWriteOperation before
     * it's created.
     *
     * @param operationType
     * @param uri
     * @param metadata
     * @param content
     * @return
     */
    protected DocumentWriteOperation build(DocumentWriteOperation.OperationType operationType, String uri, DocumentMetadataHandle metadata, AbstractWriteHandle content) {
        return new DocumentWriteOperationImpl(operationType, uri, metadata, content);
    }

    public DocumentWriteOperationBuilder withUriPrefix(String uriPrefix) {
        this.uriPrefix = uriPrefix;
        return this;
    }

    public DocumentWriteOperationBuilder withUriSuffix(String uriSuffix) {
        this.uriSuffix = uriSuffix;
        return this;
    }

    public DocumentWriteOperationBuilder withCollections(String collections) {
        this.collections = collections;
        return this;
    }

    public DocumentWriteOperationBuilder withPermissions(String permissions) {
        this.permissions = permissions;
        return this;
    }

    public DocumentWriteOperationBuilder withOperationType(DocumentWriteOperation.OperationType operationType) {
        this.operationType = operationType;
        return this;
    }

}
