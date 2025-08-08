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
