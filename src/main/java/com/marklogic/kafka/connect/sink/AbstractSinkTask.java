/*
 * Copyright (c) 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.kafka.connect.sink;

import com.marklogic.kafka.connect.ConfigUtil;
import com.marklogic.kafka.connect.MarkLogicConnectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Base class for concrete SinkTask implementations, providing some generic functionality.
 */
abstract class AbstractSinkTask extends SinkTask {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private boolean logKeys = false;
    private boolean logHeaders = false;
    protected BiConsumer<SinkRecord, Throwable> errorReporterMethod;
    public static final String MARKLOGIC_MESSAGE_FAILURE_HEADER = "marklogic-failure-type";
    public static final String MARKLOGIC_MESSAGE_EXCEPTION_MESSAGE = "marklogic-exception-message";
    public static final String MARKLOGIC_ORIGINAL_TOPIC = "marklogic-original-topic";
    public static final String MARKLOGIC_TARGET_URI = "marklogic-target-uri";
    public static final String MARKLOGIC_WRITE_FAILURE = "Write failure";
    public static final String MARKLOGIC_CONVERSION_FAILURE = "Record conversion";

    /**
     * Subclasses implement this to pull their necessary config from Kafka. Invoked by the {@code start} method.
     *
     * @param parsedConfig
     */
    protected abstract void onStart(Map<String, Object> parsedConfig);

    /**
     * Subclasses implement this to determine how to write each {@code SinkRecord}. This is invoked by the
     * {@code put} method, which subclasses can override if necessary - e.g. to provide their own behavior after all
     * records have been processed.
     *
     * @param sinkRecord
     */
    protected abstract void writeSinkRecord(SinkRecord sinkRecord);

    /**
     * Required for a Kafka task.
     *
     * @return
     */
    @Override
    public String version() {
        return MarkLogicSinkConnector.MARKLOGIC_SINK_CONNECTOR_VERSION;
    }

    /**
     * Invoked by Kafka when the connector is started by Kafka Connect.
     *
     * @param config initial configuration
     */
    @Override
    public final void start(Map<String, String> config) {
        logger.info("Starting");
        Map<String, Object> parsedConfig = MarkLogicSinkConfig.CONFIG_DEF.parse(config);
        logKeys = ConfigUtil.getBoolean(MarkLogicSinkConfig.LOGGING_RECORD_KEY, parsedConfig);
        logHeaders = ConfigUtil.getBoolean(MarkLogicSinkConfig.LOGGING_RECORD_HEADERS, parsedConfig);
        this.onStart(parsedConfig);
        logger.info("Started");
    }

    /**
     * Invoked by Kafka each time it determines that it has data to send to a connector.
     *
     * @param records the set of records to send
     */
    @Override
    public void put(Collection<SinkRecord> records) {
        records.forEach(sinkRecord -> {
            // It is not known if either of these scenarios will ever occur; it would seem that Kafka would never pass
            // a null record nor a record with a null value to a connector.
            if (sinkRecord == null) {
                logger.debug("Skipping null record");
            } else if (sinkRecord.value() == null) {
                logger.debug("Skipping record with null value");
            } else {
                logRecordBeforeWriting(sinkRecord);
                try {
                    this.writeSinkRecord(sinkRecord);
                } catch (Exception ex) {
                    // Including the stacktrace here as this could happen for a variety of reasons
                    throw new MarkLogicConnectorException("Unable to write sink record; record offset: " + sinkRecord.kafkaOffset() +
                        "cause: " + ex.getMessage(), ex);
                }
            }
        });
    }

    private void logRecordBeforeWriting(SinkRecord sinkRecord) {
        if (logKeys && sinkRecord.key() != null) {
            logger.info("Record key {}", sinkRecord.key());
        }
        if (logHeaders) {
            List<String> headers = new ArrayList<>();
            sinkRecord.headers().forEach(header -> headers.add(String.format("%s:%s", header.key(), header.value().toString())));
            logger.info("Record headers: {}", headers);
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Processing record value {} in topic {}", sinkRecord.value(), sinkRecord.topic());
        }
    }

    /**
     * Exposed for testing.
     */
    void setErrorReporterMethod(BiConsumer<SinkRecord, Throwable> errorReporterMethod) {
        this.errorReporterMethod = errorReporterMethod;
    }
}
