package com.datastax.driver.core.exceptions;

/**
 * An exception indicating that a query cannot be executed because it is
 * incorrect syntaxically, invalid, unauthorized or any other reason.
 */
public class QueryValidationException extends DriverUncheckedException {

    protected QueryValidationException(String msg) {
        super(msg);
    }
}
