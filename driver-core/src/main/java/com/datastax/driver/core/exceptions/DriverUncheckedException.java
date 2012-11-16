package com.datastax.driver.core.exceptions;

/**
 * Top level class for unchecked exceptions thrown by the driver.
 */
public class DriverUncheckedException extends RuntimeException {

    DriverUncheckedException() {
        super();
    }

    DriverUncheckedException(String message) {
        super(message);
    }

    DriverUncheckedException(Throwable cause) {
        super(cause);
    }

    DriverUncheckedException(String message, Throwable cause) {
        super(message, cause);
    }
}
