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
package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Defines how a Kafka SinkRecord is converted into a DocumentWriteOperation, which can then be
 * written to MarkLogic via a WriteBatcher or DocumentManager. Simplifies testing of this logic, as this avoids any
 * dependency on a running MarkLogic or running Kafka instance.
 */
public interface SinkRecordConverter {

    DocumentWriteOperation convert(SinkRecord sinkRecord);

}
