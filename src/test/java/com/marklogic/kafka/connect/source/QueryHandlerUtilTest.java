package com.marklogic.kafka.connect.source;

import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RawQueryDSLPlan;
import com.marklogic.client.row.RowManager;
import com.marklogic.kafka.connect.MarkLogicConnectorException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryHandlerUtilTest extends AbstractIntegrationSourceTest {

    @Test
    void invalidMaxValueQuery() {
        RowManager rowManager = getDatabaseClient().newRowManager();
        final String invalidDsl = "This should fail";
        RawQueryDSLPlan invalidQuery = rowManager.newRawQueryDSLPlan(new StringHandle(invalidDsl));

        MarkLogicConnectorException ex = assertThrows(MarkLogicConnectorException.class,
            () -> QueryHandlerUtil.executeMaxValuePlan(rowManager, invalidQuery, 0, invalidDsl));

        assertTrue(ex.getMessage().startsWith("Unable to get max constraint value; query: This should fail"),
            "If either our max value query has a bug in it, or the server has a bug in it with processing our max " +
                "value query, an exception should be thrown that informs the user that the max value query failed " +
                "along with what the query was");
    }
}
