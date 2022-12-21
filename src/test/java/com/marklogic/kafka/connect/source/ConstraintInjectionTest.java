package com.marklogic.kafka.connect.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConstraintInjectionTest extends AbstractIntegrationSourceTest {
    private static final String constraintColumn = "lucky_number";
    private static final String constraintValue = "52";

    @Test
    void testAccessorOnlyQuery() {
        String originalDsl = "op.fromView(\"Medical\", \"Authors\")";
        String expectedValue = "op.fromView(\"Medical\", \"Authors\").where(op.gt(op.col(\"" + constraintColumn + "\"), " + constraintValue + "))";
        Assertions.assertEquals(expectedValue, AbstractRowBatchBuilder.injectConstraintIntoDslQuery(originalDsl, constraintColumn, constraintValue));
    }

    @Test
    void testQueryWithLimit() {
        String originalDsl = "op.fromView(\"Medical\", \"Authors\").limit(1000)";
        String expectedValue = "op.fromView(\"Medical\",\"Authors\").where(op.gt(op.col(\"" + constraintColumn + "\"), " + constraintValue + ")).limit(1000)";
        Assertions.assertEquals(expectedValue, AbstractRowBatchBuilder.injectConstraintIntoDslQuery(originalDsl, constraintColumn, constraintValue));
    }

    @Test
    void testQueryWithLineFeed() {
        String originalDsl = "op.fromView(\"Medical\", \"Authors\")  \n    .limit(1000)";
        String expectedValue = "op.fromView(\"Medical\",\"Authors\").where(op.gt(op.col(\"" + constraintColumn + "\"), " + constraintValue + ")).limit(1000)";
        Assertions.assertEquals(expectedValue, AbstractRowBatchBuilder.injectConstraintIntoDslQuery(originalDsl, constraintColumn, constraintValue));
    }

    @Test
    void testQueryWithSpacesInSingleQuotes() {
        String originalDsl = "op.fromView(\"Medical\", \"Authors\").select([op.as(\"Last Name\", op.col(\"LastName\"), op.as(\'Fore Name\', op.col(\'ForeName\')])";
        String expectedValue = "op.fromView(\"Medical\",\"Authors\").where(op.gt(op.col(\"" + constraintColumn + "\"), " +
            constraintValue + ")).select([op.as(\"Last Name\",op.col(\"LastName\"),op.as(\'Fore Name\',op.col(\'ForeName\')])";
        Assertions.assertEquals(expectedValue, AbstractRowBatchBuilder.injectConstraintIntoDslQuery(originalDsl, constraintColumn, constraintValue));
    }
}
