package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.ExportListener;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.kafka.connect.DefaultDatabaseClientCreator;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/* ATTENTION - DO NOT USE
    THIS IS NOT READY

    This is an attempt to prototype the SourceTask using DMSDK. However, I don't think I understand the DMSDK well
    enough to do it correctly.

    I am only saving it so that DMSDK experts can look at it and laugh. And maybe eventually I can fix it.
 */
public class MarkLogicDmsdkSourceTask extends SourceTask {

    static final Integer MARKLOGIC_POLL_INTERVAL = 1000;

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicDmsdkSourceTask.class);

    private Map<String, String> config;
    private String topic;
    private String query;
    String queryCollection;
    String processedCollection;

    private DatabaseClient databaseClient;
    private DataMovementManager dataMovementManager;
    private QueryBatcher queryBatcher;
    JobTicket ticket;

    private ArrayList<SourceRecord> records = new ArrayList<>();

    @Override
    public String version() {
        return MarkLogicSourceConnector.MARKLOGIC_SOURCE_CONNECTOR_VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("MarkLogicSourceTask Starting");

        config = props;
        topic = config.get(MarkLogicSourceConfig.KAFKA_TOPIC);
        query = config.get(MarkLogicSourceConfig.QUERY);
        queryCollection = config.get(MarkLogicSourceConfig.QUERY_COLLECTION);
        processedCollection = config.get(MarkLogicSourceConfig.QUERY_PROCESSED_COLLECTION);

        databaseClient = new DefaultDatabaseClientCreator().createDatabaseClient(config);
        dataMovementManager = databaseClient.newDataMovementManager();
        final StringQueryDefinition stringQueryDefinition = databaseClient.newQueryManager().newStringDefinition();
        stringQueryDefinition.setCriteria(query);
        stringQueryDefinition.setCollections(queryCollection);
        logger.info("Query: " + query);
        logger.info("Query Collection: " + queryCollection);

        queryBatcher = dataMovementManager.newQueryBatcher(stringQueryDefinition)
                .withThreadCount(Integer.parseInt(config.get(MarkLogicSourceConfig.THREAD_COUNT)))
                .onUrisReady(getExportListener())
                .onQueryFailure(Throwable::printStackTrace);

        logger.info("MarkLogicSourceTask Started");
    }

    private ExportListener getExportListener() {
        return new ExportListener()
                .onDocumentReady(doc -> {
                    logger.info("Exporting document from MarkLogic: " + doc.getUri());
                    String documentContent = doc.getContent(new StringHandle()).get();
                    Map<String, String> sourcePartition = Collections.singletonMap("filename", doc.getUri());
                    Map<String, Integer> sourceOffset = Collections.singletonMap("position", 1);
                    records.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, documentContent));
                });
    }


    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        sleep();
        records = new ArrayList<>();
        logger.info("Querying for MarkLogic records");
        ticket = dataMovementManager.startJob(queryBatcher);
        queryBatcher.awaitCompletion();
        dataMovementManager.stopJob(ticket);
        logger.info(String.format("Finished querying MarkLogic (%d records found)", records.size()));
        return records;
    }

    private void sleep() throws InterruptedException {
        Thread.sleep(MARKLOGIC_POLL_INTERVAL);
    }

    @Override
    public void stop() {
        dataMovementManager.stopJob(queryBatcher);
    }
}
