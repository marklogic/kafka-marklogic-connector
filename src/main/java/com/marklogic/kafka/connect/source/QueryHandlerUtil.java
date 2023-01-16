package com.marklogic.kafka.connect.source;

class QueryHandlerUtil {
    private final static String ANY_QUOTES_REGEX_PATTERN = "[\"']";
    protected static String sanitize(String target) {
        return target.replaceAll(ANY_QUOTES_REGEX_PATTERN, "");
    }
}
