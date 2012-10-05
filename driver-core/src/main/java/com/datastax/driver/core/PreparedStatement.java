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
    final byte[] id;

    private PreparedStatement(Columns metadata, byte[] id) {
        this.metadata = metadata;
        this.id = id;
    }

    static PreparedStatement fromMessage(ResultMessage msg) {
        switch (msg.kind) {
            case PREPARED:
                ResultMessage.Prepared pmsg = (ResultMessage.Prepared)msg;
                Columns.Definition[] defs = new Columns.Definition[pmsg.metadata.names.size()];
                for (int i = 0; i < defs.length; i++)
                    defs[i] = Columns.Definition.fromTransportSpecification(pmsg.metadata.names.get(i));

                return new PreparedStatement(new Columns(defs), pmsg.statementId.bytes);
            case VOID:
            case ROWS:
            case SET_KEYSPACE:
                throw new RuntimeException("ResultSet received when prepared statement received was expected");
            default:
                throw new AssertionError();
        }
    }

    /**
     * Returns metadata on the bounded variables of this prepared statement.
     *
     * @return the variables bounded in this prepared statement.
     */
    public Columns variables() {
        return metadata;
    }

    /**
     * Creates a new BoundStatement object and bind its variables to the
     * provided values.
     *
     * This method is a shortcut for {@code this.newBoundStatement().bind(...)}.
     *
     * @param values the values to bind to the variables of the newly created
     * BoundStatement.
     * @return the newly created {@code BoundStatement} with its variables
     * bound to {@code values}.
     *
     * @see {@link BoundStatement#bind}
     */
    public BoundStatement bind(Object... values) {
        BoundStatement bs = new BoundStatement(this);
        return bs.bind(values);
    }

    /**
     * Creates a new {@code BoundStatement} from this prepared statement.
     *
     * @return the newly created {@code BoundStatement}.
     */
    public BoundStatement newBoundStatement() {
        return new BoundStatement(this);
    }
}
