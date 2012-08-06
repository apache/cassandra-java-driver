package com.datastax.driver.core;

public class InvalidTypeException extends RuntimeException {

    public InvalidTypeException(String msg) {
        super(msg);
    }
}
