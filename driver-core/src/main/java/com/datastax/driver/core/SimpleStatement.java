package com.datastax.driver.core;

/**
 * A simple {@code CQLStatement} implementation built directly from a query
 * string.
 */
public class SimpleStatement extends CQLStatement {

    private final String query;

    public SimpleStatement(String query) {
        this.query = query;
    }

    public String getQueryString() {
        return query;
    }
}
