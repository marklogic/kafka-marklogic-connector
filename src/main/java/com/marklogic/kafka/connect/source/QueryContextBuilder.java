package com.marklogic.kafka.connect.source;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public interface QueryContextBuilder<T> {
    QueryContext<T> newQueryContext(List<SourceRecord> newSourceRecords, String previousMaxConstraintColumnValue);
}
