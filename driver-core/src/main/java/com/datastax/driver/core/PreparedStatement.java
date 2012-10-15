package com.datastax.driver.core;

import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.transport.messages.ResultMessage;

import com.datastax.driver.core.exceptions.DriverInternalError;

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

    final ColumnDefinitions metadata;
    final MD5Digest id;

    private PreparedStatement(ColumnDefinitions metadata, MD5Digest id) {
        this.metadata = metadata;
        this.id = id;
    }

    static PreparedStatement fromMessage(ResultMessage.Prepared msg) {
        switch (msg.kind) {
            case PREPARED:
                ResultMessage.Prepared pmsg = (ResultMessage.Prepared)msg;
                ColumnDefinitions.Definition[] defs = new ColumnDefinitions.Definition[pmsg.metadata.names.size()];
                for (int i = 0; i < defs.length; i++)
                    defs[i] = ColumnDefinitions.Definition.fromTransportSpecification(pmsg.metadata.names.get(i));

                return new PreparedStatement(new ColumnDefinitions(defs), pmsg.statementId);
            default:
                throw new DriverInternalError(String.format("%s response received when prepared statement received was expected", msg.kind));
        }
    }

    /**
     * Returns metadata on the bounded variables of this prepared statement.
     *
     * @return the variables bounded in this prepared statement.
     */
    public ColumnDefinitions getVariables() {
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
     * @throws IllegalArgumentException if more {@code values} are provided
     * than there is of bound variables in this statement.
     * @throws InvalidTypeException if any of the provided value is not of
     * correct type to be bound to the corresponding bind variable.
     *
     * @see BoundStatement#bind
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
