package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.transport.messages.ResultMessage;

import com.datastax.driver.core.exceptions.DriverInternalError;

/**
 * Represents a prepared statement, a query with bound variables that has been
 * prepared (pre-parsed) by the database.
 * <p>
 * A prepared statement can be executed once concrete values has been provided
 * for the bound variables. The pair of a prepared statement and values for its
 * bound variables is a BoundStatement and can be executed (by
 * {@link Session#execute}).
 */
public class PreparedStatement {

    final ColumnDefinitions metadata;
    final MD5Digest id;

    volatile ByteBuffer routingKey;
    final int[] routingKeyIndexes;

    volatile ConsistencyLevel consistency;

    private PreparedStatement(ColumnDefinitions metadata, MD5Digest id, int[] routingKeyIndexes) {
        this.metadata = metadata;
        this.id = id;
        this.routingKeyIndexes = routingKeyIndexes;
    }

    static PreparedStatement fromMessage(ResultMessage.Prepared msg, Metadata clusterMetadata) {
        switch (msg.kind) {
            case PREPARED:
                ResultMessage.Prepared pmsg = (ResultMessage.Prepared)msg;
                ColumnDefinitions.Definition[] defs = new ColumnDefinitions.Definition[pmsg.metadata.names.size()];
                if (defs.length == 0)
                    return new PreparedStatement(new ColumnDefinitions(defs), pmsg.statementId, null);

                List<ColumnMetadata> partitionKeyColumns = null;
                int[] pkIndexes = null;
                KeyspaceMetadata km = clusterMetadata.getKeyspace(pmsg.metadata.names.get(0).ksName);
                if (km != null) {
                    TableMetadata tm = km.getTable(pmsg.metadata.names.get(0).cfName);
                    if (tm != null) {
                        partitionKeyColumns = tm.getPartitionKey();
                        pkIndexes = new int[partitionKeyColumns.size()];
                        for (int i = 0; i < pkIndexes.length; ++i)
                            pkIndexes[i] = -1;
                    }
                }

                // Note: we rely on the fact CQL queries cannot span multiple tables. If that change, we'll have to get smarter.
                for (int i = 0; i < defs.length; i++) {
                    defs[i] = ColumnDefinitions.Definition.fromTransportSpecification(pmsg.metadata.names.get(i));
                    maybeGetIndex(defs[i].getName(), i, partitionKeyColumns, pkIndexes);
                }

                return new PreparedStatement(new ColumnDefinitions(defs), pmsg.statementId, allSet(pkIndexes) ? pkIndexes : null);
            default:
                throw new DriverInternalError(String.format("%s response received when prepared statement received was expected", msg.kind));
        }
    }

    private static void maybeGetIndex(String name, int j, List<ColumnMetadata> pkColumns, int[] pkIndexes) {
        if (pkColumns == null)
            return;

        for (int i = 0; i < pkColumns.size(); ++i) {
            if (name.equals(pkColumns.get(i).getName())) {
                // We may have the same column prepared multiple times, but only pick the first value
                pkIndexes[i] = j;
                return;
            }
        }
    }

    private static boolean allSet(int[] pkColumns) {
        if (pkColumns == null)
            return false;

        for (int i = 0; i < pkColumns.length; ++i)
            if (pkColumns[i] < 0)
                return false;

        return true;
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
        if (consistency != null)
            bs.setConsistencyLevel(consistency);
        return bs.bind(values);
    }

    /**
     * Creates a new {@code BoundStatement} from this prepared statement.
     *
     * @return the newly created {@code BoundStatement}.
     */
    public BoundStatement newBoundStatement() {
        BoundStatement bs = new BoundStatement(this);
        if (consistency != null)
            bs.setConsistencyLevel(consistency);
        return bs;
    }

    /**
     * Set the routing key for this prepared statement.
     * <p>
     * This method allows to manually provide a fixed routing key for all
     * executions of this prepared statement. It is never mandatory to provide
     * a routing key through this method and this method should only be used
     * if the partition key of the prepared query is not part of the prepared
     * variables (i.e. if the partition key is fixed).
     * <p>
     * Note that if the partition key is part of the prepared variables, the
     * routing key will be automatically computed once those variables are bound.
     *
     * @param routingKey the raw (binary) value to use as routing key.
     * @return this {@code PreparedStatement} object.
     *
     * @see Query#getRoutingKey
     */
    public PreparedStatement setRoutingKey(ByteBuffer routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    /**
     * Set the routing key for this query.
     * <p>
     * See {@link #setRoutingKey(ByteBuffer)} for more information. This
     * method is a variant for when the query partition key is composite and
     * thus the routing key must be built from multiple values.
     *
     * @param routingKeyComponents the raw (binary) values to compose to obtain
     * the routing key.
     * @return this {@code PreparedStatement} object.
     *
     * @see Query#getRoutingKey
     */
    public PreparedStatement setRoutingKey(ByteBuffer... routingKeyComponents) {
        this.routingKey = SimpleStatement.compose(routingKeyComponents);
        return this;
    }

    /**
     * Sets a default consistency level for all {@code BoundStatement} created
     * from this object.
     * <p>
     * If set, any {@code BoundStatement} created through either {@link #bind} or
     * {@link #newBoundStatement}.
     *
     * @param consistency the default consistency level to set.
     * @return this {@code PreparedStatement} object.
     */
    public PreparedStatement setConsistencyLevel(ConsistencyLevel consistency) {
        this.consistency = consistency;
        return this;
    }

    /**
     * The default consistency level set through {@link #setConsistencyLevel}.
     *
     * @return the default consistency level. Returns {@code null} if no
     * consistency level has been set through this object {@code setConsistencyLevel}
     * method.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistency;
    }
}
