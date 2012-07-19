package com.datastax.driver.core;

/**
 * Represents a prepared statement, a query with bound variables that has been
 * prepared (pre-parsed) by the database.
 * <p>
 * A prepared statement can be executed once concrete values has been provided
 * for the bound variables. The pair of a prepared statement and values for its
 * bound variables is a BoundStatement and can be executed by
 * {@link Session#executePrepared}.
 */
public class PreparedStatement {

    public Columns variables() {
        return null;
    }

    public BoundStatement bind(Object... values) {
        return null;
    }

    public BoundStatement newBoundStatement() {
        return null;
    }
}
