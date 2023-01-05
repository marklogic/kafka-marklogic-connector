package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.RowBatcher;
import org.apache.kafka.common.config.ConfigException;
import org.springframework.util.StringUtils;

import java.util.Map;

import static java.lang.String.format;

public interface QueryHandler {
    void addQueryToRowBatcher(RowBatcher<?> rowBatcher, String previousMaxConstraintColumnValue);

    String getMaxConstraintColumnValue(long queryStartTimeInMillis);

    static QueryHandler newQueryHandler(DatabaseClient databaseClient, Map<String, Object> parsedConfig) {
        boolean configuredForDsl = StringUtils.hasText((String) parsedConfig.get(MarkLogicSourceConfig.DSL_QUERY));
        boolean configuredForSerialized = StringUtils.hasText((String) parsedConfig.get(MarkLogicSourceConfig.SERIALIZED_QUERY));
        if ((!(configuredForDsl || configuredForSerialized)) || (configuredForDsl && configuredForSerialized)) {
            throw new ConfigException(
                format("Either a DSL Optic query (%s) or a serialized Optic query (%s), but not both, are required",
                    MarkLogicSourceConfig.DSL_QUERY, MarkLogicSourceConfig.SERIALIZED_QUERY)
            );
        }
        return configuredForDsl ?
            new DslQueryHandler(databaseClient, parsedConfig) :
            new SerializedQueryHandler(databaseClient, parsedConfig);
    }
}
