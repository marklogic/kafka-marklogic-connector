package com.marklogic.client.ext.document;

import com.marklogic.client.io.marker.AbstractWriteHandle;

import java.util.UUID;

public class DefaultContentIdExtractor implements ContentIdExtractor {

	@Override
	public String extractId(AbstractWriteHandle content) {
		return UUID.randomUUID().toString();
	}
}
