package com.datastax.driver.core.exceptions;

/**
 * Top level class for (checked) exceptions thrown by the driver.
 */
public class DriverException extends Exception {

    public DriverException() {
        super();
    }

    public DriverException(String message) {
        super(message);
    }

    public DriverException(Throwable cause) {
        super(cause);
    }

    public DriverException(String message, Throwable cause) {
        super(message, cause);
    }
}
