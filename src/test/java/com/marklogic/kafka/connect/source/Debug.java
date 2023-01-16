package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.row.RowManager;

public class Debug {

    public static void main(String[] args) {
//        String query = "op.fromView('Medical', 'Authors').orderBy(op.asc('ID'))" +
//            ".where(op.gt(op.col('ID'), op.param('previousMaxConstraintColumnValue')))" +
//            ".orderBy(op.asc(op.col('ID')))" +
//            ".limit(op.param('rowLimit'))" +
//            ".orderBy(op.desc('ID'))" +
//            ".limit(1)" +
//            ".select([op.as('constraint', op.col('ID'))])";

//        String query = "op.fromView('Medical', 'Authors')" +
//            ".where(op.gt(op.col('ID'), op.param('previousMaxConstraintColumnValue')))" +
//            ".limit(op.param('myRowLimit'))";

        // Doesn't work
        String previousValue = "1";

        // Does work
//        int previousValue = 4;


//        String query = "op.fromView(\"Medical\", \"Authors\")" +
//            ".orderBy(op.asc(\"ID\"))" +
//            ".where(op.gt(op.col('ID'), op.param('PREVIOUS_MAX_VALUE')))" +
//            ".orderBy(op.asc(op.col('ID')))" +
//            ".limit(op.param('ROW_LIMIT'))" +
//            ".orderBy(op.desc('ID'))" +
//            ".limit(1)" +
//            ".bind(op.as('constraint', op.col('ID')))";
//            ".select([op.as('constraint', op.col('ID'))])";

        String query = "op.fromLiterals([{LastName:'Second',ID:2},{LastName:'Third',ID:3},{LastName:'First',ID:1}])" +
            ".where(op.gt(op.col('ID'), op.param('PREVIOUS_MAX_VALUE')))" +
            ".orderBy(op.asc(op.col('ID')))" +
            ".limit(op.param('ROW_LIMIT'))";

        DatabaseClient client = DatabaseClientFactory.newClient("localhost", 8019,
            new DatabaseClientFactory.DigestAuthContext("admin", "admin"));
        RowManager rm = client.newRowManager();
        PlanBuilder op = rm.newPlanBuilder();
        PlanBuilder.Plan plan = rm.newRawQueryDSLPlan(new StringHandle(query))
            .bindParam(op.param("ROW_LIMIT"), "3")
            .bindParam(op.param("PREVIOUS_MAX_VALUE"), "2");
        ;
        JacksonHandle baseHandle = new JacksonHandle();
        JacksonHandle result = rm.resultDoc(plan, baseHandle);
        System.out.println(result.get().toPrettyString());
    }
}
