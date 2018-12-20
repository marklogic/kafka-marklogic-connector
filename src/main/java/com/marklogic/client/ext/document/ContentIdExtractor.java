package com.marklogic.client.ext.document;

import com.marklogic.client.io.marker.AbstractWriteHandle;

public interface ContentIdExtractor {

	String extractId(AbstractWriteHandle content);
	
}
