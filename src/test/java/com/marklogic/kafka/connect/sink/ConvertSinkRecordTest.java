package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a DocumentWriteOperation is created correctly based on a SinkRecord.
 */
public class ConvertSinkRecordTest {

	DefaultSinkRecordConverter converter;

	@Test
	public void allPropertiesSet() {
		Map<String, String> config = new HashMap<>();
		config.put("ml.document.collections", "one,two");
		config.put("ml.document.format", "json");
		config.put("ml.document.mimeType", "application/json");
		config.put("ml.document.permissions", "manage-user,read,manage-admin,update");
		config.put("ml.document.uriPrefix", "/example/");
		config.put("ml.document.uriSuffix", ".json");
		converter = new DefaultSinkRecordConverter(config);

		DocumentWriteOperation op = converter.convert(newSinkRecord("test"));

		assertTrue(op.getUri().startsWith("/example/"));
		assertTrue(op.getUri().endsWith(".json"));
		

		DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
		Iterator<String> collections = metadata.getCollections().iterator();
		assertEquals("one", collections.next());
		assertEquals("two", collections.next());

		DocumentMetadataHandle.DocumentPermissions perms = metadata.getPermissions();
		assertEquals(DocumentMetadataHandle.Capability.READ, perms.get("manage-user").iterator().next());
		assertEquals(DocumentMetadataHandle.Capability.UPDATE, perms.get("manage-admin").iterator().next());

		StringHandle content = (StringHandle)op.getContent();
		assertEquals("test", content.get());
		assertEquals(Format.JSON, content.getFormat());
		assertEquals("application/json", content.getMimetype());
	}

	@Test
	public void noPropertiesSet() {
		converter = new DefaultSinkRecordConverter(new HashMap<>());
		converter.getDocumentWriteOperationBuilder().withContentIdExtractor(content -> "12345");

		DocumentWriteOperation op = converter.convert(newSinkRecord("doesn't matter"));

		assertEquals("12345", op.getUri());

		DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
		assertTrue(metadata.getCollections().isEmpty());
		assertTrue(metadata.getPermissions().isEmpty());
	}

	@Test
	public void binaryContent() {
		converter = new DefaultSinkRecordConverter(new HashMap<>());

		DocumentWriteOperation op = converter.convert(newSinkRecord("hello world".getBytes()));

		BytesHandle content = (BytesHandle)op.getContent();
		assertEquals("hello world".getBytes().length, content.get().length);
	}

	private SinkRecord newSinkRecord(Object value) {
		return new SinkRecord("test-topic", 1, null, null, null, value, 0);
	}
}
