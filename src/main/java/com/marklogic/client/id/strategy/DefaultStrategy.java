package com.marklogic.client.id.strategy;

import java.util.UUID;

import com.marklogic.client.io.marker.AbstractWriteHandle;


public class DefaultStrategy implements IdStrategy{
	
	@Override
	public String generateId(AbstractWriteHandle content, String topic, Integer partition, long offset) {
		return UUID.randomUUID().toString();
	}
    
} 
