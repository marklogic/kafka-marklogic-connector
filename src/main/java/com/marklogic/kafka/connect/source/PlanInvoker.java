package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Strategy interface for how a plan is invoked; "invoke" is the chosen term per the docs at
 * https://docs.marklogic.com/REST/POST/v1/rows .
 */
public interface PlanInvoker {

    /**
     * Invoke the given plan, returning a Results object containing source records that should be associated with
     * the given topic.
     *
     * @param plan
     * @param topic
     * @return
     */
    Results invokePlan(PlanBuilder.Plan plan, String topic);

    /**
     * Primary purpose of the class is to transfer the source records along with the MarkLogic server timestamp at
     * which the Plan was invoked, thus allowing for the max value of a particular column to be calculated using that
     * same timestamp.
     */
    class Results {
        private List<SourceRecord> sourceRecords;
        private long serverTimestamp;

        public Results(List<SourceRecord> sourceRecords, long serverTimestamp) {
            this.sourceRecords = sourceRecords;
            this.serverTimestamp = serverTimestamp;
        }

        public List<SourceRecord> getSourceRecords() {
            return sourceRecords;
        }

        public long getServerTimestamp() {
            return serverTimestamp;
        }
    }

    static PlanInvoker newPlanInvoker(DatabaseClient client, Map<String, Object> parsedConfig) {
        String format = (String) parsedConfig.get(MarkLogicSourceConfig.OUTPUT_FORMAT);
        MarkLogicSourceConfig.OUTPUT_TYPE outputType = StringUtils.hasText(format) ?
            MarkLogicSourceConfig.OUTPUT_TYPE.valueOf(format) :
            MarkLogicSourceConfig.OUTPUT_TYPE.JSON;

        switch (outputType) {
            case JSON:
                return new JsonPlanInvoker(client, parsedConfig);
            case CSV:
                return new CsvPlanInvoker(client, parsedConfig);
            case XML:
                return new XmlPlanInvoker(client, parsedConfig);
            default:
                throw new IllegalArgumentException("Unexpected output type: " + outputType);
        }
    }
}
