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
package com.marklogic.kafka.connect.sink.idstrategy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public class JSONPathStrategy implements IdStrategy {

    private static final Logger logger = LoggerFactory.getLogger(JSONPathStrategy.class);

    private String path;

    public JSONPathStrategy(String path) {
        this.path = path;
    }

    @Override
    public String generateId(AbstractWriteHandle content, String topic, Integer partition, long offset) {
        ObjectMapper om = new ObjectMapper();
        try {
            return om.readTree(content.toString()).at(path).asText();
        } catch (IOException e) {
            logger.warn("IOException. Not creating JSONPATH URI, instead generating UUID");
            return UUID.randomUUID().toString();
        }
    }

}
