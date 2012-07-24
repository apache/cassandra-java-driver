package com.datastax.driver.core;

import org.apache.cassandra.transport.messages.ResultMessage;

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

    final Columns metadata;
    final int id;

    private PreparedStatement(Columns metadata, int id) {
        this.metadata = metadata;
        this.id = id;
    }

    static PreparedStatement fromMessage(ResultMessage.Prepared msg) {

        Columns.Definition[] defs = new Columns.Definition[msg.metadata.names.size()];
        for (int i = 0; i < defs.length; i++)
            defs[i] = Columns.Definition.fromTransportSpecification(msg.metadata.names.get(i));

        return new PreparedStatement(new Columns(defs), msg.statementId);
    }

    /**
     * Returns metadata on the bounded variables of this prepared statement.
     *
     * @return the variables bounded in this prepared statement.
     */
    public Columns variables() {
        return metadata;
    }

    public BoundStatement bind(Object... values) {
        BoundStatement bs = new BoundStatement(this);
        return bs.bind(values);
    }

    public BoundStatement newBoundStatement() {
        return new BoundStatement(this);
    }
}
