package com.datastax.driver.core.exceptions;

/**
 * A specific invalid query exception that indicates that the query is invalid
 * because of some configuration problem.
 * <p>
 * This is generally throw by query that manipulate the schema (CREATE and
 * ALTER) when the required configuration options are invalid.
 */
public class InvalidConfigurationInQueryException extends InvalidQueryException {

    public InvalidConfigurationInQueryException(String msg) {
        super(msg);
    }
}
