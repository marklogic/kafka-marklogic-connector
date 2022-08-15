package com.marklogic.client.id.strategy;

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
        String id = "";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            String tmp = topic + partition.toString() + String.valueOf(offset);
            id = bytesToHex(md.digest(tmp.getBytes()));
            return id;
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
