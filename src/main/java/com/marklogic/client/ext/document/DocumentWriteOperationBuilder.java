package com.marklogic.client.ext.document;

import java.io.IOException;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;

public class DocumentWriteOperationBuilder {

	private DocumentWriteOperation.OperationType operationType = DocumentWriteOperation.OperationType.DOCUMENT_WRITE;
	private String uriPrefix;
	private String uriSuffix;
	private String collections;
	private String permissions;

	private ContentIdExtractor contentIdExtractor = new DefaultContentIdExtractor();
	private String idStrategy;
	private String path;
	

	public DocumentWriteOperation build(AbstractWriteHandle content, DocumentMetadataHandle metadata, String topic, Integer partition, long offset ) throws IOException {
		if (content == null) {
			throw new NullPointerException("'content' must not be null");
		}

		if (hasText(collections)) {
			metadata.getCollections().addAll(collections.trim().split(","));
		}
		if (hasText(permissions)) {
			new DefaultDocumentPermissionsParser().parsePermissions(permissions.trim(), metadata.getPermissions());
		}
		String uri = "";
		if ("JSONPATH".equals(idStrategy)) {
			uri = buildUri(content,path.trim().split(",")[0]);
		}
		else if ("HASH".equals(idStrategy)) {
			uri = buildUri(content,path.trim().split(","));
		}
		else if ("UUID".equals(idStrategy)) {
			uri = buildUri(content);
		}
		else if ("KAFKA_META_WITH_SLASH".equals(idStrategy)) {
			uri = buildUri(content,topic,partition,offset,idStrategy);
		}
		else if ("KAFKA_META_HASHED".equals(idStrategy)) {
			uri = buildUri(content,topic,partition,offset,idStrategy);
		}
		else {
			uri = buildUri(content);
		}
			
		return build(operationType, uri, metadata, content);
	}

	protected String buildUri(AbstractWriteHandle content) throws IOException  {
		String uri = contentIdExtractor.extractId(content);
		if (hasText(uriPrefix)) {
			uri = uriPrefix + uri;
		}
		if (hasText(uriSuffix)) {
			uri += uriSuffix;
		}
		return uri;
	}
	
	protected String buildUri(AbstractWriteHandle content,String path) throws IOException  {
		String uri = contentIdExtractor.extractId(content,path);
		if (hasText(uriPrefix)) {
			uri = uriPrefix + uri;
		}
		if (hasText(uriSuffix)) {
			uri += uriSuffix;
		}
		return uri;
	}
	
	protected String buildUri(AbstractWriteHandle content,String[] paths) throws IOException  {
		String uri = contentIdExtractor.extractId(content,paths);
		if (hasText(uriPrefix)) {
			uri = uriPrefix + uri;
		}
		if (hasText(uriSuffix)) {
			uri += uriSuffix;
		}
		return uri;
	}
	
	protected String buildUri(AbstractWriteHandle content,String topic, Integer partition, long offset, String idStrategy) throws IOException  {
		String uri = contentIdExtractor.extractId(content,topic,partition,offset,idStrategy);
		if (hasText(uriPrefix)) {
			uri = uriPrefix + uri;
		}
		if (hasText(uriSuffix)) {
			uri += uriSuffix;
		}
		return uri;
	}

	/**
	 * Exists to give a subclass a chance to further customize the contents of a DocumentWriteOperation before
	 * it's created.
	 *
	 * @param operationType
	 * @param uri
	 * @param metadata
	 * @param content
	 * @return
	 */
	protected DocumentWriteOperation build(DocumentWriteOperation.OperationType operationType, String uri, DocumentMetadataHandle metadata, AbstractWriteHandle content) {
		return new DocumentWriteOperationImpl(operationType, uri, metadata, content);
	}

	private boolean hasText(String val) {
		return val != null && val.trim().length() > 0;
	}

	public DocumentWriteOperationBuilder withUriPrefix(String uriPrefix) {
		this.uriPrefix = uriPrefix;
		return this;
	}

	public DocumentWriteOperationBuilder withUriSuffix(String uriSuffix) {
		this.uriSuffix = uriSuffix;
		return this;
	}

	public DocumentWriteOperationBuilder withCollections(String collections) {
		this.collections = collections;
		return this;
	}

	public DocumentWriteOperationBuilder withPermissions(String permissions) {
		this.permissions = permissions;
		return this;
	}

	public DocumentWriteOperationBuilder withOperationType(DocumentWriteOperation.OperationType operationType) {
		this.operationType = operationType;
		return this;
	}

	public DocumentWriteOperationBuilder withContentIdExtractor(ContentIdExtractor contentIdExtractor) {
		this.contentIdExtractor = contentIdExtractor;
		return this;
	}

	public DocumentWriteOperationBuilder withIdStrategy(String strategy) {
		this.idStrategy = strategy;
		return this;
	}
	
	public DocumentWriteOperationBuilder withIdStrategyPath(String path) {
		this.path = path;
		return this;
	}

}
