package com.marklogic.kafka.connect.sink;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
/*For Avro conversion */
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.document.DocumentWriteOperationBuilder;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import java.util.Collections;
import org.apache.kafka.connect.json.JsonConverter;

/**
 * Handles converting a SinkRecord into a DocumentWriteOperation via the properties in the given config map.
 */
public class DefaultSinkRecordConverter implements SinkRecordConverter {

	String converter = "STRING"; 
	private static final Converter JSON_CONVERTER;
	private static final Logger logger = LoggerFactory.getLogger(MarkLogicSinkTask.class);
	static {
	    JSON_CONVERTER = new JsonConverter();
	    JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
		}

	private DocumentWriteOperationBuilder documentWriteOperationBuilder;
	private Format format;
	private String mimeType;
	private Boolean addTopicToCollections = false; 
	
	public DefaultSinkRecordConverter(Map<String, String> kafkaConfig) {
	
		String val = kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS_ADD_TOPIC);
		if (val != null && val.trim().length() > 0) {
			addTopicToCollections = Boolean.parseBoolean(val.trim());
		}
		
		documentWriteOperationBuilder = new DocumentWriteOperationBuilder()
			.withCollections(kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS))
			.withPermissions(kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS))
			.withUriPrefix(kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_URI_PREFIX))
			.withUriSuffix(kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX))
			.withIdStrategy(kafkaConfig.get(MarkLogicSinkConfig.ID_STRATEGY))
			.withIdStrategyPath(kafkaConfig.get(MarkLogicSinkConfig.ID_STRATEGY_PATH))
			;

		val = kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_FORMAT);
		if (val != null && val.trim().length() > 0) {
			format = Format.valueOf(val.toUpperCase());
		}
		val = kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_MIMETYPE);
		if (val != null && val.trim().length() > 0) {
			mimeType = val;
		}
	}
	
	@Override
	public DocumentWriteOperation convert(SinkRecord sinkRecord) throws IOException{
		return documentWriteOperationBuilder.build(toContent(sinkRecord), 
				                                   addTopicToCollections(sinkRecord.topic(), addTopicToCollections ),
				                                   sinkRecord.topic(),
				                                   sinkRecord.kafkaPartition(),
				                                   sinkRecord.kafkaOffset()
				                                   );
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
		/* Determine the converter */
		if (schema != null && value instanceof Struct) {
			/* Avro or JSON with schema, ignore schema, handle only the value */
			converter = "AVRO_OR_JSON_WITH_SCHEMA";
			logger.info("Avro or JsonWithSchema document received. Converting to byte[].");
			final String payload = new String(JSON_CONVERTER.fromConnectData(record.topic(), schema, value), StandardCharsets.UTF_8);
			value = payload.getBytes();
		}
		
        if (value instanceof Map) {
        	converter = "JSON_WITHOUT_SCHEMA";
        	logger.info("Json document received. Converting to byte[]");
        	value = new String (JSON_CONVERTER.fromConnectData(record.topic(), null, value)).getBytes();
        }
        
		if (value instanceof String) {
			logger.info("String document received. Converting to byte[]");
			converter = "STRING";
        }
		
		if (value instanceof byte[]) {
			logger.info("Byte[] received. Proceeding with MarkLogic operations.");
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
