package com.datastax.driver.core;

public class BoundStatement {

    /**
     * Returns the prepared statement on which this BoundStatement is based.
     *
     * @return the prepared statement on which this BoundStatement is based.
     */
    public PreparedStatement preparedStatement() {
        return null;
    }

    /**
     * Returns whether all variables have been bound to values in thi
     * BoundStatement.
     *
     * @return whether all variables are bound.
     */
    public boolean ready() {
        return false;
    }

    public BoundStatement bind(Object... values) {
        return null;
    }

    public BoundStatement setBool(int i, boolean v) {
        return null;
    }

    public BoundStatement setBool(String name, boolean v) {
        return null;
    }

    public BoundStatement setInt(int i, int v) {
        return null;
    }

    public BoundStatement setInt(String name, int v) {
        return null;
    }

    // ...
}
