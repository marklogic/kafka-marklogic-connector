package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import org.apache.kafka.connect.sink.SinkRecord;
 
import org.junit.jupiter.api.Test;


import java.util.*;
import java.io.IOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verifies that a DocumentWriteOperation is created correctly based on a SinkRecord.
 */
public class ConvertSinkRecordTest {

	DefaultSinkRecordConverter converter;
	MarkLogicSinkTask markLogicSinkTask = new MarkLogicSinkTask();
	private JsonObject doc1, doc2, doc3;

	@Test
	public void allPropertiesSet() throws IOException {
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
	public void noPropertiesSet() throws IOException {
		converter = new DefaultSinkRecordConverter(new HashMap<>());
		//converter.getDocumentWriteOperationBuilder().withContentIdExtractor(content -> "12345");
		
		DocumentWriteOperation op = converter.convert(newSinkRecord("doesn't matter"));

		assertNotNull(op.getUri());
		assertEquals(36,op.getUri().length());

		DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
		assertTrue(metadata.getCollections().isEmpty());
		assertTrue(metadata.getPermissions().isEmpty());
	}

	@Test
	public void UriWithKafkaMetaData() throws IOException {
		converter = new DefaultSinkRecordConverter(new HashMap<>());
		converter.getDocumentWriteOperationBuilder().withIdStrategy("KAFKA_META_WITH_SLASH");
		
		DocumentWriteOperation op = converter.convert(newSinkRecord("doesn't matter"));

		assertEquals("test-topic/1/0",op.getUri());

		DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
		assertTrue(metadata.getCollections().isEmpty());
		assertTrue(metadata.getPermissions().isEmpty());
	}
	
	@Test
	public void UriWithJsonPath() throws IOException {
		 JsonParser parser = new JsonParser();
		 doc1 = parser.parse("{\"f1\":\"100\"}").getAsJsonObject();
		converter = new DefaultSinkRecordConverter(new HashMap<>());
		converter.getDocumentWriteOperationBuilder().withIdStrategy("JSONPATH").withIdStrategyPath("/f1");
		
		DocumentWriteOperation op = converter.convert(newSinkRecord(doc1));
		
		assertNotNull(op.getUri());
		assertEquals("100",op.getUri());

		DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
		assertTrue(metadata.getCollections().isEmpty());
		assertTrue(metadata.getPermissions().isEmpty());
	}
	
	@Test
	public void UriWithHashedJsonPaths() throws IOException {
		 JsonParser parser = new JsonParser();
		 doc1 = parser.parse("{\"f1\":\"100\",\"f2\":\"200\"}").getAsJsonObject();
		converter = new DefaultSinkRecordConverter(new HashMap<>());
		converter.getDocumentWriteOperationBuilder().withIdStrategy("HASH").withIdStrategyPath("/f1,/f2");
		
		DocumentWriteOperation op = converter.convert(newSinkRecord(doc1));

		assertNotNull(op.getUri());
		assertEquals(32,op.getUri().length()); //Checking the length. Alternative is to create a MD5 of 100200 and check asserton the values.

		DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
		assertTrue(metadata.getCollections().isEmpty());
		assertTrue(metadata.getPermissions().isEmpty());
	}
	
	@Test
	public void UriWithHashedKafkaMeta() throws IOException {
		 JsonParser parser = new JsonParser();
		 doc1 = parser.parse("{\"f1\":\"100\",\"f2\":\"200\"}").getAsJsonObject();
		converter = new DefaultSinkRecordConverter(new HashMap<>());
		converter.getDocumentWriteOperationBuilder().withIdStrategy("KAFKA_META_HASHED");
		
		DocumentWriteOperation op = converter.convert(newSinkRecord(doc1));

		assertNotNull(op.getUri());
		assertEquals(32,op.getUri().length()); //Checking the length. Alternative is to create a MD5 of 100200 and check asserton the values.

		DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
		assertTrue(metadata.getCollections().isEmpty());
		assertTrue(metadata.getPermissions().isEmpty());
	}
	
	@Test
	public void binaryContent() throws IOException{
		converter = new DefaultSinkRecordConverter(new HashMap<>());

		DocumentWriteOperation op = converter.convert(newSinkRecord("hello world".getBytes()));

		BytesHandle content = (BytesHandle)op.getContent();
		assertEquals("hello world".getBytes().length, content.get().length);
	}

	@Test
	public void emptyContent() {
		final Collection<SinkRecord> records = new ArrayList<>();
		records.add(newSinkRecord(null));
		markLogicSinkTask.put(records);
	}

	@Test
	public void nullContent() {
		final Collection<SinkRecord> records = new ArrayList<>();
		records.add(null);
		markLogicSinkTask.put(records);
	}

	private SinkRecord newSinkRecord(Object value) {
		return new SinkRecord("test-topic", 1, null, null, null, value, 0);
	}
}
