/*
 * Copyright (c) 2019-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ext.helper.LoggingObject;
import org.springframework.util.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public abstract class ConstraintValueStore extends LoggingObject {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String constraintColumn;

    ConstraintValueStore(String constraintColumn) {
        this.constraintColumn = constraintColumn;
    }

    public abstract void storeConstraintState(String previousMaxConstraintColumnValue, int lastRowCount);

    public abstract String retrievePreviousMaxConstraintColumnValue();

    protected String buildConstraintState(String previousMaxConstraintColumnValue, int lastRowCount) throws JsonProcessingException {
        ConstraintState constraintState = new ConstraintState(constraintColumn, previousMaxConstraintColumnValue, lastRowCount);
        return objectMapper.writeValueAsString(constraintState);
    }

    static ConstraintValueStore newConstraintValueStore(DatabaseClient databaseClient, Map<String, Object> parsedConfig) {
        String constraintColumn = (String) parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME);
        if (StringUtils.hasText(constraintColumn)) {
            String constraintStorageUri = (String) parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_STORAGE_URI);
            if (StringUtils.hasText(constraintStorageUri)) {
                return new MarkLogicConstraintValueStore(databaseClient, constraintStorageUri, constraintColumn, parsedConfig);
            } else {
                return new InMemoryConstraintValueStore(constraintColumn);
            }
        } else {
            return null;
        }
    }

    private static class ConstraintState {
        private final String marklogicKafkaConstraintLastUpdated;
        private final String marklogicKafkaConstraintColumnName;
        private final String marklogicKafkaConstraintLastValue;
        private final String marklogicKafkaConstraintRowCount;

        ConstraintState(String constraintColumn, String lastValue, Integer lastRowCount) {
            marklogicKafkaConstraintLastUpdated = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date());
            marklogicKafkaConstraintColumnName = constraintColumn;
            marklogicKafkaConstraintLastValue = lastValue;
            marklogicKafkaConstraintRowCount = lastRowCount.toString();
        }

        public String getMarklogicKafkaConstraintLastUpdated() {
            return marklogicKafkaConstraintLastUpdated;
        }

        public String getMarklogicKafkaConstraintColumnName() {
            return marklogicKafkaConstraintColumnName;
        }

        public String getMarklogicKafkaConstraintLastValue() {
            return marklogicKafkaConstraintLastValue;
        }

        public String getMarklogicKafkaConstraintRowCount() {
            return marklogicKafkaConstraintRowCount;
        }
    }
}
