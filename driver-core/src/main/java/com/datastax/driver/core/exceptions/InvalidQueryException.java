package com.datastax.driver.core.exceptions;

/**
 * Indicates a syntactically correct but invalid query.
 */
public class InvalidQueryException extends QueryValidationException {

    public InvalidQueryException(String msg) {
        super(msg);
    }
}
