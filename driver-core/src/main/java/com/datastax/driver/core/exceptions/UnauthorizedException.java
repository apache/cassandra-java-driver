package com.datastax.driver.core.exceptions;

/**
 * Indicates that a query cannot be performed due to the authorisation
 * restrictions of the logged user.
 */
public class UnauthorizedException extends QueryValidationException {

    public UnauthorizedException(String msg) {
        super(msg);
    }
}
