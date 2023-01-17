package com.marklogic.kafka.connect;

/**
 * Created because sonarqube doesn't approve of generic exceptions like RuntimeException being used.
 */
public class MarkLogicConnectorException extends RuntimeException {

    public MarkLogicConnectorException(String message) {
        super(message);
    }

    public MarkLogicConnectorException(String message, Throwable cause) {
        super(message, cause);
    }
}
