package com.datastax.driver.core.schemabuilder;

import com.google.common.base.Strings;

public abstract class SchemaStatement {

    static final String OPEN_PAREN = "(";
    static final String CLOSE_PAREN = ")";
    static final String SPACE = " ";
    static final String SEPARATOR = ", ";
    static final String DOT = ".";

    abstract String buildInternal();

    protected static void validateNotEmpty(String columnName, String label) {
        if (Strings.isNullOrEmpty(columnName)) {
            throw new IllegalArgumentException(label + " should not be null or blank");
        }
    }

    protected static void validateNotNull(Object value, String label) {
        if (value == null) {
            throw new IllegalArgumentException(label + " should not be null");
        }
    }
}
