package com.marklogic.kafka.connect.source;

import com.marklogic.client.datamovement.RowBatcher;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public interface RowBatcherBuilder<T> {
    RowBatcher<T> newRowBatcher(List<SourceRecord> newSourceRecords);
}
