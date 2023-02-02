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
