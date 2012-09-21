package com.datastax.driver.core.exceptions;

/**
 * Error during a truncation operation.
 */
// TODO: should that extend QueryExecutionException. In theory yes, but that's
// probably not part of what you want to deal with when you catch
// QueryExecutionException?
public class TruncateException extends DriverException {

    public TruncateException(String msg) {
        super(msg);
    }
}
