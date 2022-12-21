package com.marklogic.kafka.connect.source;

import com.marklogic.client.datamovement.RowBatcher;

public class QueryContext<T> {
    private RowBatcher<T> rowBatcher;
    private final String currentQuery;

    QueryContext(RowBatcher<T> rowBatcher, String currentQuery) {
        this.rowBatcher = rowBatcher;
        this.currentQuery = currentQuery;
    }

    public RowBatcher<?> getRowBatcher() {
        return rowBatcher;
    }

    public String getCurrentQuery() {
        return currentQuery;
    }

    public void clearRowBatcher() {
        rowBatcher = null;
    }
}
