package kafka.connect.marklogic.sink;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.StringHandle;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 
 * @author Sanju Thomas
 * @author pbarber
 *
 */
public class MarkLogicSinkTask extends SinkTask {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicSinkTask.class);

    private int timeout;
    private Map<String, String> config;
    private int maxRetires;
    private int remainingRetries;

    protected DatabaseClient client;
    DocumentManager documentManager;

    private int batchSize;
    private BufferedRecords bufferedRecords;
    private DataMovementManager dataMovementManager;

    @Override
    public void start(final Map<String, String> config) {
        logger.info("start called!");
        this.config = config;
        this.timeout = Integer.valueOf(config.get(MarkLogicSinkConfig.RETRY_BACKOFF_MS));
        this.maxRetires = Integer.valueOf(config.get(MarkLogicSinkConfig.MAX_RETRIES));
        this.remainingRetries = maxRetires;

        client = DatabaseClientFactory.newClient(config.get(MarkLogicSinkConfig.CONNECTION_HOST),
                Integer.valueOf(config.get(MarkLogicSinkConfig.CONNECTION_PORT)),
                new DatabaseClientFactory.DigestAuthContext(config.get(MarkLogicSinkConfig.CONNECTION_USER),
                        config.get(MarkLogicSinkConfig.CONNECTION_PASSWORD)));
        documentManager = client.newDocumentManager();
        bufferedRecords = new BufferedRecords();
        dataMovementManager = client.newDataMovementManager();
        batchSize = Integer.valueOf(config.get(MarkLogicSinkConfig.BATCH_SIZE));
    }

    @Override
    public void stop() {
        logger.info("stop called!");
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            logger.debug("Empty record collection to process");
            return;
        }

        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        logger.debug("Received {} records. kafka coordinates from record: Topic - {}, Partition - {}, Offset - {}",
                        recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());

        try {
            records.forEach(r -> bufferedRecords.buffer(r));
            flush();
        } catch (final RetriableException e) {
            if (maxRetires > 0 && remainingRetries == 0) {
                throw new ConnectException("Retries exhausted, ending the task. Manual restart is required.");
            }else{
                logger.warn("Setting the task timeout to {} ms upon RetriableException", timeout);
                context.timeout(timeout);
                remainingRetries--;
                throw e;
            }
        }
        this.remainingRetries = maxRetires;
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        currentOffsets.forEach((k, v) -> logger.debug("Flush - Topic {}, Partition {}, Offset {}, Metadata {}",
                k.topic(), k.partition(), v.offset(), v.metadata()));
    }

    public String version() {
        return MarkLogicSinkConnector.MARKLOGIC_CONNECTOR_VERSION;
    }

    protected String url(){
        return UUID.randomUUID().toString() + "." + config.get(MarkLogicSinkConfig.EXTENSION);
    }

    private void flush() {
        final WriteBatcher batcher = dataMovementManager.newWriteBatcher();

        batcher.withBatchSize(batchSize).withThreadCount(8).onBatchFailure((b, t) -> {
            logger.error("batch write failed {}", t);
            throw new RetriableException(t.getMessage());
        });

        dataMovementManager.startJob(batcher);
        this.bufferedRecords.forEach(r -> {
            String value = r.toString();
            logger.info("received value {}, and collection {}", value, r.topic());
            batcher.add(url(), new StringHandle((String) r.value()));
        });

        batcher.flushAndWait();
        dataMovementManager.stopJob(batcher);
        bufferedRecords.clear();
    }

    protected InputStreamHandle handle(String value){
        return new InputStreamHandle(new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     *
     * Buffer the Records until the batch size reached.
     *
     */
    class BufferedRecords extends ArrayList<SinkRecord> {

        private static final long serialVersionUID = 1L;

        void buffer(final SinkRecord r){
            add(r);
            if(batchSize <= size()){
                logger.debug("buffer size is {}", batchSize);
                flush();
                logger.debug("flushed the buffer");
            }
        }
    }

}
