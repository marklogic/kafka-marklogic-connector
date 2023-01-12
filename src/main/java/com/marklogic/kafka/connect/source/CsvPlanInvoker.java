package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

class CsvPlanInvoker implements PlanInvoker {

    private DatabaseClient client;
    private KeyGenerator keyGenerator;

    public CsvPlanInvoker(DatabaseClient client, Map<String, Object> parsedConfig) {
        this.client = client;
        this.keyGenerator = KeyGenerator.newKeyGenerator(parsedConfig);
    }

    @Override
    public Results invokePlan(PlanBuilder.Plan plan, String topic) {
        StringHandle baseHandle = new StringHandle().withFormat(Format.TEXT).withMimetype("text/csv");
        StringHandle result = client.newRowManager().resultDoc(plan, baseHandle);
        List<SourceRecord> records = new ArrayList<>();

        if (result.get() != null) {
            try (BufferedReader reader = new BufferedReader(new StringReader(result.get()))) {
                String headers = reader.readLine();
                AtomicLong rowNumber = new AtomicLong(1);
                reader.lines().forEach(line -> {
                    String key = keyGenerator.generateKey(rowNumber.getAndIncrement());
                    String newDocument = headers + "\n" + line;
                    SourceRecord newRecord = new SourceRecord(null, null, topic, null, key, null, newDocument);
                    records.add(newRecord);
                });
            } catch (IOException ex) {
                throw new RuntimeException("Unable to parse CSV results: " + ex.getMessage(), ex);
            }
        }

        return new Results(records, baseHandle.getServerTimestamp());
    }
}
