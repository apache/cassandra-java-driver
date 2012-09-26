package com.datastax.driver.core.exceptions;

/**
 * Top level class for unchecked exceptions thrown by the driver.
 */
public class DriverUncheckedException extends RuntimeException {

    public DriverUncheckedException() {
        super();
    }

    public DriverUncheckedException(String message) {
        super(message);
    }

    public DriverUncheckedException(Throwable cause) {
        super(cause);
    }

    public DriverUncheckedException(String message, Throwable cause) {
        super(message, cause);
    }
}
