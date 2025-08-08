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
package com.marklogic.kafka.connect.sink.idstrategy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public class HashedJSONPathsStrategy implements IdStrategy {

    private static final Logger logger = LoggerFactory.getLogger(HashedJSONPathsStrategy.class);

    private String[] paths;

    public HashedJSONPathsStrategy(String[] paths) {
        this.paths = paths;
    }

    @Override
    public String generateId(AbstractWriteHandle content, String topic, Integer partition, long offset) {
        ObjectMapper om = new ObjectMapper();
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-512");
            JsonNode node = om.readTree(content.toString());
            StringBuilder id = new StringBuilder();
            for (int i = 0; i < paths.length; i++) {
                id.append(node.at(paths[i].trim()).asText());
            }
            return bytesToHex(md.digest(id.toString().getBytes()));
        } catch (IOException e) {
            logger.warn("IOException. Not creating MD5 URI, instead generating UUID");
            return UUID.randomUUID().toString();
        } catch (NoSuchAlgorithmException e) {
            logger.warn("NoSuchAlgorithmException. Not creating MD5 URI, instead generating UUID");
            return UUID.randomUUID().toString();
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

}
