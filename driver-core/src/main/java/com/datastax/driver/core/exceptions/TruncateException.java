package com.datastax.driver.core.exceptions;

/**
 * Error during a truncation operation.
 */
public class TruncateException extends QueryExecutionException {

    public TruncateException(String msg) {
        super(msg);
    }
}
