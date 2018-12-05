package kafka.connect.marklogic.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class MarkLogicSinkConnector extends SinkConnector {

	private Map<String, String> config;
	public static final String MARKLOGIC_CONNECTOR_VERSION = "1.0";

	private static final Logger logger = LoggerFactory.getLogger(MarkLogicSinkConnector.class);

	@Override
	public ConfigDef config() {
		return MarkLogicSinkConfig.CONFIG_DEF;
	}

	@Override
	public void start(final Map<String, String> arg0) {
		config = arg0;
	}

	@Override
	public void stop() {
		logger.info("stop called");
	}

	@Override
	public Class<? extends Task> taskClass() {
		return MarkLogicSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(final int taskCount) {
	    final List<Map<String, String>> configs = new ArrayList<>(taskCount);
	    for (int i = 0; i < taskCount; ++i) {
	      configs.add(config);
	    }
	    return configs;
	}

	@Override
	public String version() {
		return MARKLOGIC_CONNECTOR_VERSION;
	}

}
