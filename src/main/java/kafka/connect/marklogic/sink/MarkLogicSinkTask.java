package kafka.connect.marklogic.sink;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.kafka.connect.DefaultDatabaseClientCreator;
import com.marklogic.kafka.connect.sink.DefaultSinkRecordConverter;
import com.marklogic.kafka.connect.sink.SinkRecordConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class MarkLogicSinkTask extends SinkTask {

	private static final Logger logger = LoggerFactory.getLogger(MarkLogicSinkTask.class);

	private DatabaseClient databaseClient;
	private DataMovementManager dataMovementManager;
	private WriteBatcher writeBatcher;
	private SinkRecordConverter sinkRecordConverter;

	@Override
	public void start(final Map<String, String> config) {
		logger.info("Starting");

		sinkRecordConverter = new DefaultSinkRecordConverter(config);

		databaseClient = new DefaultDatabaseClientCreator().createDatabaseClient(config);
		dataMovementManager = databaseClient.newDataMovementManager();
		writeBatcher = dataMovementManager.newWriteBatcher()
			.withBatchSize(Integer.valueOf(config.get(MarkLogicSinkConfig.DMSDK_BATCH_SIZE)))
			.withThreadCount(Integer.valueOf(config.get(MarkLogicSinkConfig.DMSDK_THREAD_COUNT)));

		dataMovementManager.startJob(writeBatcher);

		logger.info("Started");
	}

	/**
	 * Creates a new DatabaseClient based on the configuration properties found in the given map.
	 *
	 * @param config
	 * @return
	 */
	@Override
	public void stop() {
		logger.info("Stopping");
		if (writeBatcher != null) {
			writeBatcher.flushAndWait();
			dataMovementManager.stopJob(writeBatcher);
		}
		if (databaseClient != null) {
			databaseClient.release();
		}
		logger.info("Stopped");
	}

	/**
	 * This is doing all the work of writing to MarkLogic, which includes calling flushAsync on the WriteBatcher.
	 * Alternatively, could move the flushAsync call to an overridden flush() method. Kafka defaults to flushing every
	 * 60000ms - this can be configured via the offset.flush.interval.ms property.
	 * <p>
	 * Because this is calling flushAsync, the batch size won't come into play unless the incoming collection has a
	 * size equal to or greater than the batch size.
	 *
	 * @param records
	 */
	@Override
	public void put(final Collection<SinkRecord> records) {
		if (records.isEmpty()) {
			return;
		}

		records.forEach(record -> {
			if (logger.isDebugEnabled()) {
				logger.debug("Processing record value {} in topic {}", record.value(), record.topic());
			}
			writeBatcher.add(sinkRecordConverter.convert(record));
		});

		writeBatcher.flushAsync();
	}

	public String version() {
		return MarkLogicSinkConnector.MARKLOGIC_CONNECTOR_VERSION;
	}
}
