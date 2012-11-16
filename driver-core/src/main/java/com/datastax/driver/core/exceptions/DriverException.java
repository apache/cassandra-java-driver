package com.datastax.driver.core.exceptions;

/**
 * Top level class for (checked) exceptions thrown by the driver.
 */
public class DriverException extends Exception {

    DriverException() {
        super();
    }

    DriverException(String message) {
        super(message);
    }

    DriverException(Throwable cause) {
        super(cause);
    }

    DriverException(String message, Throwable cause) {
        super(message, cause);
    }
}
