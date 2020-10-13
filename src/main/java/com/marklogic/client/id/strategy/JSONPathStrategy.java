package com.marklogic.client.id.strategy;

import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.io.marker.AbstractWriteHandle;

public class JSONPathStrategy implements IdStrategy{
	
	private static final Logger logger = LoggerFactory.getLogger(JSONPathStrategy.class);
	
	private String path;
	
	public JSONPathStrategy (String path) {
		this.path = path;
	}
	
	@Override
	public String generateId(AbstractWriteHandle content, String topic, Integer partition, long offset) {
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
	
} 
