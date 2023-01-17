package com.marklogic.client.id.strategy;

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
