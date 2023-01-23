package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.client.row.RowManager;
import com.marklogic.kafka.connect.ConfigUtil;

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
        if (value != null && value.trim().length() > 0) {
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
