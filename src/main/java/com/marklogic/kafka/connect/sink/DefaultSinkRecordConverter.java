package com.marklogic.kafka.connect.sink;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.id.strategy.IdStrategyFactory;
import com.marklogic.client.id.strategy.IdStrategy;
import com.marklogic.client.ext.document.DocumentWriteOperationBuilder;
import com.marklogic.client.document.RecordContent;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;

/**
 * Handles converting a SinkRecord into a DocumentWriteOperation via the properties in the given config map.
 */
public class DefaultSinkRecordConverter implements SinkRecordConverter {

	private static final Converter JSON_CONVERTER;
	static {
	    JSON_CONVERTER = new JsonConverter();
	    JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
		}

	private DocumentWriteOperationBuilder documentWriteOperationBuilder;
	private Format format;
	private String mimeType;
	private Boolean addTopicToCollections = false; 
	private IdStrategy idStrategy = null;
	
	public DefaultSinkRecordConverter(Map<String, Object> parsedConfig) {

		String val = (String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS_ADD_TOPIC);
		if (val != null && val.trim().length() > 0) {
			addTopicToCollections = Boolean.parseBoolean(val.trim());
		}
		
		documentWriteOperationBuilder = new DocumentWriteOperationBuilder()
			.withCollections((String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS))
			.withPermissions((String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS))
			.withUriPrefix((String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_URI_PREFIX))
			.withUriSuffix((String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX))
			;

		val = (String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_FORMAT);
		if (val != null && val.trim().length() > 0) {
			format = Format.valueOf(val.toUpperCase());
		}
		val = (String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_MIMETYPE);
		if (val != null && val.trim().length() > 0) {
			mimeType = val;
		}
		//Get the correct ID or URI generation strategy based on the configuration
		idStrategy = IdStrategyFactory.getIdStrategy(parsedConfig);
	}
	
	@Override
	public DocumentWriteOperation convert(SinkRecord sinkRecord) throws IOException{
		RecordContent recordContent = new RecordContent();
		AbstractWriteHandle content = toContent(sinkRecord);
		recordContent.setContent(content);
		recordContent.setAdditionalMetadata(addTopicToCollections(sinkRecord.topic(), addTopicToCollections ));
		recordContent.setId(idStrategy.generateId(content, sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset()));
		return documentWriteOperationBuilder.build(recordContent);
	}
	
	/**
	 * 
	 *
	 * @param topic, addTopicToCollections
	 * @return  metadata 
	 */
	protected DocumentMetadataHandle addTopicToCollections (String topic, Boolean addTopicToCollections) {
		DocumentMetadataHandle metadata = new DocumentMetadataHandle();
			if (addTopicToCollections) {
				metadata.getCollections().add(topic);
			} 
		return metadata ;
	}
	
	/**
	 * Constructs an appropriate handle based on the value of the SinkRecord.
	 *
	 * @param record
	 * @return
	 */
	protected AbstractWriteHandle toContent(SinkRecord record) {
		if ((record == null) || (record.value() == null)) {
			throw new NullPointerException("'record' must not be null, and must have a value.");
		}
		Object value = record.value();
		Schema schema = record.valueSchema();
		
		/* Determine the converter based on the value of SinkRecord*/
		if (schema != null && value instanceof Struct) {
			/* Avro, ProtoBuf or JSON with schema, ignore schema, handle only the value */
			value = new String(JSON_CONVERTER.fromConnectData(record.topic(), schema, value), StandardCharsets.UTF_8).getBytes();
		}
		
        if (value instanceof Map) {
        	value = new String (JSON_CONVERTER.fromConnectData(record.topic(), null, value)).getBytes();
        }

        if (value instanceof byte[]) {
			BytesHandle content = new BytesHandle((byte[]) value);
			if (format != null) {
				content.withFormat(format);
			}
			if (mimeType != null) {
				content.withMimetype(mimeType);
			}
			return content;
		}

		StringHandle content = new StringHandle(record.value().toString());
		if (format != null) {
			content.withFormat(format);
		}
		if (mimeType != null) {
			content.withMimetype(mimeType);
		}
		return content;
	}

	public DocumentWriteOperationBuilder getDocumentWriteOperationBuilder() {
		return documentWriteOperationBuilder;
	}

	public void setDocumentWriteOperationBuilder(DocumentWriteOperationBuilder documentWriteOperationBuilder) {
		this.documentWriteOperationBuilder = documentWriteOperationBuilder;
	}
}
