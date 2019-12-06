package com.marklogic.kafka.connect;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.Map;

public class MarkLogicConnectionTest {

    @ClassRule
    public static GenericContainer alpineContainer =
            new GenericContainer("ml-initialized:9.0-8.2")
                    .withExposedPorts(8000, 8001, 8002);

    DefaultDatabaseClientCreator creator = new DefaultDatabaseClientCreator();
    Map<String, String> config = new HashMap<>();

    @BeforeEach
    public void setup() {
        config.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        config.put(MarkLogicSinkConfig.CONNECTION_DATABASE, "Documents");
    }

    @Test
    void testMarkLogicConnectivity() throws Exception {
        alpineContainer.start();
        config.put(MarkLogicSinkConfig.CONNECTION_PORT, alpineContainer.getMappedPort(8000).toString());

        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "digest");
        config.put(MarkLogicSinkConfig.CONNECTION_USERNAME, "admin");
        config.put(MarkLogicSinkConfig.CONNECTION_PASSWORD, "admin");

        DatabaseClient databaseClient = new DefaultDatabaseClientCreator().createDatabaseClient(config);
        QueryManager queryMgr = databaseClient.newQueryManager();
        String query = "";
        StringQueryDefinition stringQueryDefinition = queryMgr.newStringDefinition();
        stringQueryDefinition.setCriteria(query);

        SearchHandle results = queryMgr.search(stringQueryDefinition, new SearchHandle());
    }
}
