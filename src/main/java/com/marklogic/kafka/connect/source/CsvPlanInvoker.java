package com.marklogic.kafka.connect.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RowManager;
import com.marklogic.kafka.connect.MarkLogicConnectorException;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class CsvPlanInvoker implements PlanInvoker {

    private final DatabaseClient client;
    private final String keyColumn;
    private final CsvMapper csvMapper;

    public CsvPlanInvoker(DatabaseClient client, Map<String, Object> parsedConfig) {
        this.client = client;
        this.csvMapper = new CsvMapper();
        String value = (String) parsedConfig.get(MarkLogicSourceConfig.KEY_COLUMN);
        if (value != null && value.trim().length() > 0) {
            this.keyColumn = value;
        } else {
            this.keyColumn = null;
        }
    }

    @Override
    public Results invokePlan(PlanBuilder.Plan plan, String topic) {
        StringHandle baseHandle = new StringHandle().withFormat(Format.TEXT).withMimetype("text/csv");
        RowManager mgr = client.newRowManager();
        mgr.setDatatypeStyle(RowManager.RowSetPart.HEADER);
        StringHandle result = mgr.resultDoc(plan, baseHandle);
        List<SourceRecord> records = new ArrayList<>();

        if (result.get() != null) {
            try (BufferedReader reader = new BufferedReader(new StringReader(result.get()))) {
                String headers = reader.readLine();
                Optional<Integer> keyColumnIndex = getIndexOfKeyColumn(headers);
                reader.lines().forEach(line -> {
                    String key = getKeyValueFromRow(keyColumnIndex, line);
                    String newDocument = headers + "\n" + line;
                    System.out.println("DOC: " + newDocument);
                    records.add(new SourceRecord(null, null, topic, null, key, null, newDocument));
                });
            } catch (IOException ex) {
                throw new MarkLogicConnectorException("Unable to parse CSV results: " + ex.getMessage(), ex);
            }
        }

        return new Results(records, baseHandle.getServerTimestamp());
    }

    private Optional<Integer> getIndexOfKeyColumn(String headerLine) {
        if (keyColumn != null) {
            ArrayNode headerNames;
            try {
                headerNames = (ArrayNode) csvMapper.readTree(headerLine);
            } catch (JsonProcessingException e) {
                throw new MarkLogicConnectorException(
                    String.format("Unable to parse CSV; line: %s; cause: %s", headerLine, e.getMessage()));
            }
            for (int i = 0; i < headerNames.size(); i++) {
                if (keyColumn.equals(headerNames.get(i).asText())) {
                    return Optional.of(i);
                }
            }
        }
        return Optional.empty();
    }

    private String getKeyValueFromRow(Optional<Integer> keyColumnIndex, String line) {
        if (keyColumnIndex.isPresent()) {
            try {
                JsonNode columns = csvMapper.readTree(line);
                return columns.get(keyColumnIndex.get()).asText();
            } catch (JsonProcessingException e) {
                throw new MarkLogicConnectorException(String.format("Unable to read CSV; line: %s; cause: %s",
                    line, e.getMessage()));
            }
        }
        return null;
    }
}
