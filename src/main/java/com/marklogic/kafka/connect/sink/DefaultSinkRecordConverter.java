package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.document.DocumentWriteOperationBuilder;
import com.marklogic.client.io.BytesHandle;
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

	public DefaultSinkRecordConverter(Map<String, String> kafkaConfig) {
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
		return documentWriteOperationBuilder.build(toContent(sinkRecord));
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
