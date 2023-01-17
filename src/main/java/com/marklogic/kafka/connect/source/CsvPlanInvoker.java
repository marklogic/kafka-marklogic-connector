package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.kafka.connect.MarkLogicConnectorException;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class CsvPlanInvoker implements PlanInvoker {

    private DatabaseClient client;
    private Map<String, Object> parsedConfig;

    public CsvPlanInvoker(DatabaseClient client, Map<String, Object> parsedConfig) {
        this.client = client;
        this.parsedConfig = parsedConfig;
    }

    @Override
    public Results invokePlan(PlanBuilder.Plan plan, String topic) {
        StringHandle baseHandle = new StringHandle().withFormat(Format.TEXT).withMimetype("text/csv");
        StringHandle result = client.newRowManager().resultDoc(plan, baseHandle);
        List<SourceRecord> records = new ArrayList<>();

        if (result.get() != null) {
            KeyGenerator keyGenerator = KeyGenerator.newKeyGenerator(parsedConfig, baseHandle.getServerTimestamp());
            try (BufferedReader reader = new BufferedReader(new StringReader(result.get()))) {
                String headers = reader.readLine();
                reader.lines().forEach(line -> {
                    String newDocument = headers + "\n" + line;
                    records.add(new SourceRecord(null, null, topic, null, keyGenerator.generateKey(), null, newDocument));
                });
            } catch (IOException ex) {
                throw new MarkLogicConnectorException("Unable to parse CSV results: " + ex.getMessage(), ex);
            }
        }

        return new Results(records, baseHandle.getServerTimestamp());
    }
}
