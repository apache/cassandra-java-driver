package com.datastax.driver.core.exceptions;

/**
 * An unexpected error happened internally.
 *
 * This should never be raise and indicates a bug (either in the driver or in
 * Cassandra).
 */
public class DriverInternalError extends RuntimeException {

    public DriverInternalError(String message) {
        super(message);
    }

    public DriverInternalError(Throwable cause) {
        super(cause);
    }

    public DriverInternalError(String message, Throwable cause) {
        super(message, cause);
    }

}
