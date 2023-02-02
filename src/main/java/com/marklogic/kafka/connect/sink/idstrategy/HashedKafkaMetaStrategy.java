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

import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public class HashedKafkaMetaStrategy implements IdStrategy {

    private static final Logger logger = LoggerFactory.getLogger(HashedKafkaMetaStrategy.class);

    @Override
    public String generateId(AbstractWriteHandle content, String topic, Integer partition, long offset) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-512");
            String tmp = topic + partition.toString() + offset;
            return bytesToHex(md.digest(tmp.getBytes()));
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
