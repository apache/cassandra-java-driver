package com.datastax.driver.core.exceptions;

/**
 * Exception related to the execution of a query.
 *
 * This correspond to the exception that Cassandra throw when a (valid) query
 * cannot be executed (TimeoutException, UnavailableException, ...).
 */
public class QueryExecutionException extends DriverException {

    protected QueryExecutionException(String msg) {
        super(msg);
    }
}
