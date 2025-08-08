/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.kafka.connect.source.DocumentWriteOperationBuilder;
import com.marklogic.kafka.connect.source.RecordContent;
import com.marklogic.kafka.connect.sink.idstrategy.IdStrategy;
import com.marklogic.kafka.connect.sink.idstrategy.IdStrategyFactory;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.kafka.connect.ConfigUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

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
    private final boolean addTopicToCollections;
    private final boolean includeKafkaMetadata;
    private final boolean includeKafkaHeaders;
    private final String kafkaHeadersPrefix;

    private final IdStrategy idStrategy;

    public DefaultSinkRecordConverter(Map<String, Object> parsedConfig) {
        this.addTopicToCollections = ConfigUtil.getBoolean(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS_ADD_TOPIC, parsedConfig);
        this.includeKafkaMetadata = ConfigUtil.getBoolean(MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_METADATA, parsedConfig);
        this.includeKafkaHeaders = ConfigUtil.getBoolean(MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_HEADERS, parsedConfig);
        String configPrefixValue = (String) parsedConfig.get(MarkLogicSinkConfig.DMSDK_INCLUDE_KAFKA_HEADERS_PREFIX);
        this.kafkaHeadersPrefix = (configPrefixValue == null) ? "" : configPrefixValue;

        documentWriteOperationBuilder = new DocumentWriteOperationBuilder()
            .withCollections((String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS))
            .withPermissions((String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS))
            .withUriPrefix((String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_URI_PREFIX))
            .withUriSuffix((String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX));

        String val = (String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_FORMAT);
        if (StringUtils.hasText(val)) {
            this.format = Format.valueOf(val.toUpperCase());
        }

        val = (String) parsedConfig.get(MarkLogicSinkConfig.DOCUMENT_MIMETYPE);
        if (StringUtils.hasText(val)) {
            this.mimeType = val;
        }

        this.idStrategy = IdStrategyFactory.getIdStrategy(parsedConfig);
    }

    @Override
    public DocumentWriteOperation convert(SinkRecord sinkRecord) {
        RecordContent recordContent = new RecordContent();
        AbstractWriteHandle content = toContent(sinkRecord);
        recordContent.setContent(content);
        recordContent.setAdditionalMetadata(buildAdditionalMetadata(sinkRecord));
        recordContent.setId(idStrategy.generateId(content, sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset()));
        return documentWriteOperationBuilder.build(recordContent);
    }

    private DocumentMetadataHandle buildAdditionalMetadata(SinkRecord sinkRecord) {
        SinkRecordMetadataHandle metadata = new SinkRecordMetadataHandle(sinkRecord);
        if (this.addTopicToCollections) {
            metadata.getCollections().add(sinkRecord.topic());
        }
        if (this.includeKafkaMetadata || this.includeKafkaHeaders) {
            DocumentMetadataHandle.DocumentMetadataValues values = metadata.getMetadataValues();
            addKafkaMetadataToDocumentMetadata(sinkRecord, values);
            if (this.includeKafkaHeaders) {
                sinkRecord.headers().forEach(
                    header -> values.add(kafkaHeadersPrefix + header.key(), header.value().toString())
                );
            }
        }
        return metadata;
    }

    private void addKafkaMetadataToDocumentMetadata(SinkRecord sinkRecord, DocumentMetadataHandle.DocumentMetadataValues values) {
        if (this.includeKafkaMetadata) {
            Object key = sinkRecord.key();
            if (key != null) {
                values.add("kafka-key", key.toString());
            }
            values.add("kafka-offset", sinkRecord.kafkaOffset() + "");
            Integer partition = sinkRecord.kafkaPartition();
            if (partition != null) {
                values.add("kafka-partition", partition + "");
            }
            Long timestamp = sinkRecord.timestamp();
            if (timestamp != null) {
                values.add("kafka-timestamp", timestamp + "");
            }
            values.add("kafka-topic", sinkRecord.topic());
        }
    }

    /**
     * Constructs an appropriate handle based on the value of the SinkRecord.
     *
     * @param sinkRecord
     * @return
     */
    private AbstractWriteHandle toContent(SinkRecord sinkRecord) {
        if (sinkRecord == null || sinkRecord.value() == null) {
            throw new IllegalArgumentException("Sink record must not be null and must have a value");
        }

        Object value = sinkRecord.value();
        Schema schema = sinkRecord.valueSchema();

        /* Determine the converter based on the value of SinkRecord*/
        if (schema != null && value instanceof Struct) {
            /* Avro, ProtoBuf or JSON with schema, ignore schema, handle only the value */
            value = new String(JSON_CONVERTER.fromConnectData(sinkRecord.topic(), schema, value), StandardCharsets.UTF_8).getBytes();
        }

        if (value instanceof Map) {
            value = new String(JSON_CONVERTER.fromConnectData(sinkRecord.topic(), null, value)).getBytes();
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

        StringHandle content = new StringHandle(sinkRecord.value().toString());
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
