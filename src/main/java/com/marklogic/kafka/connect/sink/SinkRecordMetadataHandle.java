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
package com.marklogic.kafka.connect.sink;

import com.marklogic.client.io.DocumentMetadataHandle;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkRecordMetadataHandle extends DocumentMetadataHandle {

    private SinkRecord sinkRecord;

    SinkRecordMetadataHandle(SinkRecord sinkRecord) {
        super();
        this.sinkRecord = sinkRecord;
    }

    public SinkRecord getSinkRecord() {
        return sinkRecord;
    }
}
