package com.datastax.driver.core.exceptions;

/**
 * Indicates a syntax error in a query.
 */
public class SyntaxError extends QueryValidationException {

    public SyntaxError(String msg) {
        super(msg);
    }
}
