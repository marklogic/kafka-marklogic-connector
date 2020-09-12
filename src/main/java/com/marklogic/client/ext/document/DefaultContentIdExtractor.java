package com.marklogic.client.ext.document;

import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.kafka.connect.sink.MarkLogicSinkTask;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;

public class DefaultContentIdExtractor implements ContentIdExtractor {
	private static final Logger logger = LoggerFactory.getLogger(MarkLogicSinkTask.class);

	@Override
	public String extractId(AbstractWriteHandle content) {
		return UUID.randomUUID().toString();
	}

	@Override
	public String extractId(AbstractWriteHandle content, String path) {
		ObjectMapper om = new ObjectMapper();
		try {
			JsonNode node = om.readTree(content.toString());
			String id = node.at(path).asText();
			return id;
		}
		catch (IOException e) {
			logger.warn("IOException. Not creating JSONPATH URI, instead generating UUID");
			return UUID.randomUUID().toString();
		}
	}

	@Override
	public String extractId(AbstractWriteHandle content, String[] paths) {
		ObjectMapper om = new ObjectMapper();
		String valueString = "";
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			JsonNode node = om.readTree(content.toString());
			for (int i=0; i<paths.length; i++) {
				valueString = valueString + node.at(paths[i].trim()).asText();
			}
			String id = bytesToHex(md.digest(valueString.getBytes()));
			return id;
		}
		catch (IOException e) {
			logger.warn("IOException. Not creating MD5 URI, instead generating UUID");
			return UUID.randomUUID().toString();
		} 
		catch (NoSuchAlgorithmException e) {
			logger.warn("NoSuchAlgorithmException. Not creating MD5 URI, instead generating UUID");
			return UUID.randomUUID().toString();
		}
	}
	
	@Override
	public String extractId(AbstractWriteHandle content, String topic, Integer partition, long offset, String idStrategy) {
		String id = "";
		if ("KAFKA_META_WITH_SLASH".equals(idStrategy)) {
			id = topic + "/" + partition.toString() + "/" + String.valueOf(offset);
			return id;
		}
		else if ("KAFKA_META_HASHED".equals(idStrategy)) {
			try {
				MessageDigest md = MessageDigest.getInstance("MD5");
				String tmp = topic + partition.toString() + String.valueOf(offset);
				id = bytesToHex(md.digest(tmp.getBytes()));
				return id;
			}
			catch (NoSuchAlgorithmException e) {
				logger.warn("NoSuchAlgorithmException. Not creating MD5 URI, instead generating UUID");
				return UUID.randomUUID().toString();
			}
		}
		else {
			logger.warn("Not a valid URI Strategy, Generating UUID");
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
