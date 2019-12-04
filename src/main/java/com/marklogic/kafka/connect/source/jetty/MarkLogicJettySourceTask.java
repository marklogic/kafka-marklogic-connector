package com.marklogic.kafka.connect.source.jetty;

import com.marklogic.kafka.connect.source.MessageQueue;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MarkLogicJettySourceTask extends SourceTask {

    private static Logger logger = LoggerFactory.getLogger(MarkLogicJettySourceTask.class);

    private Map<String, String> config;
    private MessageQueue queue = MessageQueue.getInstance();

    @Override
    public String version() {
        return MarkLogicSourceConnector.MARKLOGIC_CONNECTOR_VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("in MarkLogicJettySourceTask start()");
        config = props;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        //logger.info("in MarkLogicJettySourceTask poll()");

        List<SourceRecord> records = new ArrayList<>();
        List<MessageQueue.Message> messages = queue.getQueue();
        synchronized (messages) {
            Iterator<MessageQueue.Message> it = messages.iterator();
            while (it.hasNext()) {
                MessageQueue.Message msg = it.next();
                logger.info("found message: {}|{}|{}", msg.getTopic(), msg.getMimeType(), msg.getPayload());
                records.add(new SourceRecord(null, null, msg.getTopic(), Schema.STRING_SCHEMA, msg.getPayload()));
            }
            messages.clear();
        }

        return records;
    }

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        super.commitRecord(record);
    }

    @Override
    public void stop() {
        logger.info("in MarkLogicJettySourceTask stop()");
    }
}
