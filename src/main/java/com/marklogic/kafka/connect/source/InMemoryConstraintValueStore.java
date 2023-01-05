package com.marklogic.kafka.connect.source;

public class InMemoryConstraintValueStore extends ConstraintValueStore {
    private String previousMaxConstraintColumnValue = null;

    InMemoryConstraintValueStore(String constraintColumn) {
        super(constraintColumn);
    }

    @Override
    public void storeConstraintState(String previousMaxConstraintColumnValue, int lastRowCount) {
        this.previousMaxConstraintColumnValue = previousMaxConstraintColumnValue;
    }

    @Override
    public String retrievePreviousMaxConstraintColumnValue() {
        return previousMaxConstraintColumnValue;
    }
}
