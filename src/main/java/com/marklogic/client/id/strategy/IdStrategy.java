package com.marklogic.client.id.strategy;

import java.util.UUID;
import com.marklogic.client.io.marker.AbstractWriteHandle;

public interface IdStrategy {
	default String generateId(AbstractWriteHandle content, String topic, Integer partition, long offset) {
		return UUID.randomUUID().toString();
	}
} 
