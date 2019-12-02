package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.document.DocumentWriteOperationBuilder;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.DocumentMetadataHandle.DocumentCollections;
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
	private Boolean addTopicToCollections = false ; 
	
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
	
	/*
	 * method to check if the uses want the topic names to set as MarkLogic
	 * collections
	 * 
	 * @param SinkRecord
	 * 
	 * @return collections comma seperated collections string
	 * 
	 * public String setCollection (SinkRecord sinkRecord){ if
	 * (addTopicToCollections) { return sinkRecord.topic() + ","+ mlcollection; }
	 * else { return mlcollection; } }
	 */
	@Override
	public DocumentWriteOperation convert(SinkRecord sinkRecord) {
		return documentWriteOperationBuilder.build(toContent(sinkRecord), sinkRecord.topic(), addTopicToCollections);
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
