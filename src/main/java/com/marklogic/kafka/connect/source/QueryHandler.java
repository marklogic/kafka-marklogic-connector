package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import org.apache.kafka.common.config.ConfigException;
import org.springframework.util.StringUtils;

import java.util.Map;

import static java.lang.String.format;

/**
 * "Handles" a query in terms of modifying it to account for a constraint column and also retrieving the max value for
 * that constraint column based on the user's query.
 *
 * Note that implementations are expected to NOT be thread-safe; these are intended to be single-use objects that
 * maintain state and should not be reused.
 */
public interface QueryHandler {

    /**
     * @param previousMaxConstraintColumnValue contains the maximum value of the constrain column from the previous call
     * to the poll() method
     * @return a Plan based on the user's original query which is then modified if the previous max constraint column
     * value is not null so that the user's query will only retrieve rows higher than that value
     */
    PlanBuilder.Plan newPlan(String previousMaxConstraintColumnValue);

    /**
     *
     * @param serverTimestamp contains the timestamp of the most recent execution of the query
     * @return the max value for the user's constraint column based on the given MarkLogic server timestamp,
     * which is assumed to be the timestamp at which the connector retrieved rows
     */
    String getMaxConstraintColumnValue(long serverTimestamp, String previousMaxConstraintColumnValue);

    /**
     *
     * @return the String representation of the current query. The current query is the original user query, but possibly
     * with a constraint value injected.
     */
    String getCurrentQuery();

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
