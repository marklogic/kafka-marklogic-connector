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
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RowManager;
import org.springframework.util.StringUtils;

import java.util.Map;


public class DslQueryHandler extends LoggingObject implements QueryHandler {

    private static final String VALUE_SANITIZATION_PATTERN = "[\"'\\(\\)]";

    private final DatabaseClient databaseClient;
    private final String userDslQuery;
    private final String constraintColumnName;
    private final Integer rowLimit;

    private String currentDslQuery;

    public DslQueryHandler(DatabaseClient databaseClient, Map<String, Object> parsedConfig) {
        this.databaseClient = databaseClient;
        this.userDslQuery = (String) parsedConfig.get(MarkLogicSourceConfig.DSL_QUERY);
        this.constraintColumnName = (String) parsedConfig.get(MarkLogicSourceConfig.CONSTRAINT_COLUMN_NAME);
        rowLimit = (Integer) parsedConfig.get(MarkLogicSourceConfig.ROW_LIMIT);
    }

    @Override
    public PlanBuilder.Plan newPlan(String previousMaxConstraintColumnValue) {
        currentDslQuery = appendConstraintAndOrderByToQuery(previousMaxConstraintColumnValue);
        if (rowLimit > 0) {
            currentDslQuery += ".limit(" + rowLimit + ")";
        }
        logger.debug("DSL query: {}", currentDslQuery);
        return databaseClient.newRowManager().newRawQueryDSLPlan(new StringHandle(currentDslQuery));
    }

    protected String appendConstraintAndOrderByToQuery(String previousMaxConstraintColumnValue) {
        String constrainedDsl = userDslQuery;
        if (StringUtils.hasText(constraintColumnName)) {
            String constraintPhrase = "";
            if (StringUtils.hasText(previousMaxConstraintColumnValue)) {
                String sanitizedValue = previousMaxConstraintColumnValue.replaceAll(VALUE_SANITIZATION_PATTERN, "");
                constraintPhrase = String.format(".where(op.gt(op.col('%s'), '%s'))", constraintColumnName, sanitizedValue);
            }
            constraintPhrase += ".orderBy(op.asc(op.col('" + constraintColumnName + "')))";
            constrainedDsl = userDslQuery + constraintPhrase;
        }
        return constrainedDsl;
    }

    @Override
    public String getMaxConstraintColumnValue(long serverTimestamp) {
        String maxValueQuery = buildMaxValueDslQuery();
        logger.debug("Query for max constraint value: {}", maxValueQuery);
        RowManager rowMgr = databaseClient.newRowManager();
        return QueryHandlerUtil.executeMaxValuePlan(rowMgr, rowMgr.newRawQueryDSLPlan(new StringHandle(maxValueQuery)),
            serverTimestamp, maxValueQuery);
    }

    private String buildMaxValueDslQuery() {
        return String.format("%s" +
            ".orderBy(op.desc(op.col('%s')))" +
            ".limit(1)" +
            ".select([op.as('constraint', op.col('%s'))])",
            currentDslQuery, constraintColumnName, constraintColumnName);
    }

    public String getCurrentQuery() {
        return currentDslQuery;
    }
}
