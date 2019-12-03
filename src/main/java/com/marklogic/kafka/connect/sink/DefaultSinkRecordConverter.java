package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.document.DocumentWriteOperationBuilder;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.apache.kafka.connect.sink.SinkRecord;
import java.util.Map;

/**
 * Handles converting a SinkRecord into a DocumentWriteOperation via the properties in the given config map.
 */
public class DefaultSinkRecordConverter implements SinkRecordConverter {

	private DocumentWriteOperationBuilder documentWriteOperationBuilder;
	private Format format;
	private String mimeType;
	private Boolean addTopicToCollections = true; 
	
	public DefaultSinkRecordConverter(Map<String, String> kafkaConfig) {
		
		addTopicToCollections = Boolean.parseBoolean((kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS_ADD_TOPIC)).trim());
		
		documentWriteOperationBuilder = new DocumentWriteOperationBuilder()
			.withCollections(kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS))
			.withPermissions(kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS))
			.withUriPrefix(kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_URI_PREFIX))
			.withUriSuffix(kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX));

		String val = kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_FORMAT);
		if (val != null && val.trim().length() > 0) {
			format = Format.valueOf(val.toUpperCase());
		}
		val = kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_MIMETYPE);
		if (val != null && val.trim().length() > 0) {
			mimeType = val;
		}
	}
	
	@Override
	public DocumentWriteOperation convert(SinkRecord sinkRecord) {
		return documentWriteOperationBuilder.build(toContent(sinkRecord), addTopicToCollections(sinkRecord.topic(), addTopicToCollections ) );
	}

	/**
	 * 
	 *
	 * @param topic, addTopicToCollections
	 * @return  metadata 
	 */
	public DocumentMetadataHandle addTopicToCollections (String topic, Boolean addTopicToCollections) {
		DocumentMetadataHandle metadata = new DocumentMetadataHandle();
			if (addTopicToCollections) {
				metadata.getCollections().addAll(topic);
			} else {
				metadata.getCollections().addAll();
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
		Object value = record.value();
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
