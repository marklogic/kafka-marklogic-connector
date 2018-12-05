package kafka.connect.marklogic.sink;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.io.InputStreamHandle;
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

    private Map<String, String> config;
    private int timeout;
    private int maxRetires;
    private int batchSize;
    private int remainingRetries;

    private BufferedRecords bufferedRecords;
    private DataMovementManager dataMovementManager;

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicSinkTask.class);

    @Override
    public void start(final Map<String, String> config) {
        logger.debug("MarkLogicSinkTask start called!");
        this.config = config;
        this.timeout = Integer.valueOf(config.get(MarkLogicSinkConfig.RETRY_BACKOFF_MS));
        this.maxRetires = Integer.valueOf(config.get(MarkLogicSinkConfig.MAX_RETRIES));
        this.remainingRetries = maxRetires;
        bufferedRecords = new BufferedRecords();

        DatabaseClient client = DatabaseClientFactory.newClient(config.get(MarkLogicSinkConfig.CONNECTION_HOST),
                Integer.valueOf(config.get(MarkLogicSinkConfig.CONNECTION_PORT)),
                new DatabaseClientFactory.DigestAuthContext(config.get(MarkLogicSinkConfig.CONNECTION_USER),
                        config.get(MarkLogicSinkConfig.CONNECTION_PASSWORD)));
        dataMovementManager = client.newDataMovementManager();
        batchSize = Integer.valueOf(config.get(MarkLogicSinkConfig.BATCH_SIZE));
    }

    @Override
    public void stop() {
        logger.debug("MarkLogicSinkTask stop called!");
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
            records.forEach(record -> bufferedRecords.buffer(record));
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
        currentOffsets.forEach((key, value) ->
                logger.debug("Flush - Topic {}, Partition {}, Offset {}, Metadata {}", key.topic(),
                        key.partition(), value.offset(), value.metadata())
        );
    }

    private void flush() {
        final WriteBatcher batcher = dataMovementManager.newWriteBatcher();

        batcher.withBatchSize(batchSize).withThreadCount(8).onBatchFailure((batch, throwable) -> {
            logger.error("batch write failed {}", throwable);
            throw new RetriableException(throwable.getMessage());
        });

        dataMovementManager.startJob(batcher);
        bufferedRecords.forEach(record -> {
            String value = record.toString();
            logger.info("received value {}, and collection {}", value, record.topic());
            batcher.add(url(), handle((String) record.value()));
        });

        batcher.flushAndWait();
        dataMovementManager.stopJob(batcher);
        bufferedRecords.clear();
    }

    public String version() {
        return MarkLogicSinkConnector.MARKLOGIC_CONNECTOR_VERSION;
    }

    private String url(){
        return UUID.randomUUID().toString() + "." + config.get(MarkLogicSinkConfig.EXTENSION);
    }

    private InputStreamHandle handle(String value){
        return new InputStreamHandle(new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     *
     * Buffer the Records until the batch size reached.
     *
     */
    class BufferedRecords extends ArrayList<SinkRecord> {

        private static final long serialVersionUID = 1L;

        void buffer(final SinkRecord record){
            add(record);
            if(batchSize <= size()){
                logger.debug("buffer size is {}", batchSize);
                flush();
                logger.debug("flushed the buffer");
            }
        }
    }

}
