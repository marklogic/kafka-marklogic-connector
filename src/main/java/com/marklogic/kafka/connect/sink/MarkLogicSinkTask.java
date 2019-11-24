package com.marklogic.kafka.connect.sink;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.kafka.connect.DefaultDatabaseClientCreator;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * Performs the actual work associated with ingesting new documents into MarkLogic based on data received via the
 * "put" method.
 */
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
			.withBatchSize(Integer.parseInt(config.get(MarkLogicSinkConfig.DMSDK_BATCH_SIZE)))
			.withThreadCount(Integer.parseInt(config.get(MarkLogicSinkConfig.DMSDK_THREAD_COUNT)));

		ServerTransform transform = buildServerTransform(config);
		if (transform != null) {
			writeBatcher.withTransform(transform);
		}

		dataMovementManager.startJob(writeBatcher);

		logger.info("Started");
	}

	/**
	 * Builds a REST ServerTransform object based on the DMSDK parameters in the given config. If no transform name
	 * is configured, then null will be returned.
	 *
	 * @param config - The complete configuration object including any transform parameters.
	 * @return - The ServerTransform that will operate on each record, or null
	 */
	protected ServerTransform buildServerTransform(final Map<String, String> config) {
		String transform = config.get(MarkLogicSinkConfig.DMSDK_TRANSFORM);
		if (transform != null && transform.trim().length() > 0) {
			ServerTransform t = new ServerTransform(transform);
			String params = config.get(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS);
			if (params != null && params.trim().length() > 0) {
				String delimiter = config.get(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS_DELIMITER);
				if (delimiter != null && delimiter.trim().length() > 0) {
					String[] tokens = params.split(delimiter);
					for (int i = 0; i < tokens.length; i += 2) {
						if (i + 1 >= tokens.length) {
							throw new IllegalArgumentException(String.format("The value of the %s property does not have an even number of " +
								"parameter names and values; property value: %s", MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS, params));
						}
						t.addParameter(tokens[i], tokens[i + 1]);
					}
				} else {
					logger.warn(String.format("Unable to apply transform parameters to transform: %s; please set the " +
						"delimiter via the %s property", transform, MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS_DELIMITER));
				}
			}
			return t;
		}
		return null;
	}

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
	 * @param records - The records retrieved from Kafka
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
			if (record.value() != null) {
				writeBatcher.add(sinkRecordConverter.convert(record));
			} else {
				logger.info("Skipping record with null value");
			}
		});

		writeBatcher.flushAsync();
	}

	public String version() {
		return MarkLogicSinkConnector.MARKLOGIC_SINK_CONNECTOR_VERSION;
	}
}
