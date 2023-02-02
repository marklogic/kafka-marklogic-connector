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
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.client.row.RowManager;
import com.marklogic.kafka.connect.ConfigUtil;
import org.springframework.util.StringUtils;

import java.util.Map;

/**
 * Support class for concrete classes that implement PlanInvoker.
 */
abstract class AbstractPlanInvoker extends LoggingObject {

    protected final DatabaseClient client;
    protected final String keyColumn;
    protected final boolean includeColumnTypes;

    protected AbstractPlanInvoker(DatabaseClient client, Map<String, Object> parsedConfig) {
        this.client = client;
        String value = (String) parsedConfig.get(MarkLogicSourceConfig.KEY_COLUMN);
        if (StringUtils.hasText(value)) {
            this.keyColumn = value;
        } else {
            this.keyColumn = null;
        }
        this.includeColumnTypes = ConfigUtil.getBoolean(MarkLogicSourceConfig.INCLUDE_COLUMN_TYPES, parsedConfig);
    }

    protected final RowManager newRowManager() {
        RowManager mgr = client.newRowManager();
        if (includeColumnTypes) {
            mgr.setDatatypeStyle(RowManager.RowSetPart.ROWS);
        } else {
            mgr.setDatatypeStyle(RowManager.RowSetPart.HEADER);
        }
        return mgr;
    }
}
