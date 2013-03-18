/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
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
    final String query;
    final String queryKeyspace;

    volatile ByteBuffer routingKey;
    final int[] routingKeyIndexes;

    volatile ConsistencyLevel consistency;

    private PreparedStatement(ColumnDefinitions metadata, MD5Digest id, int[] routingKeyIndexes, String query, String queryKeyspace) {
        this.metadata = metadata;
        this.id = id;
        this.routingKeyIndexes = routingKeyIndexes;
        this.query = query;
        this.queryKeyspace = queryKeyspace;
    }

    static PreparedStatement fromMessage(ResultMessage.Prepared msg, Metadata clusterMetadata, String query, String queryKeyspace) {
        switch (msg.kind) {
            case PREPARED:
                ResultMessage.Prepared pmsg = (ResultMessage.Prepared)msg;
                ColumnDefinitions.Definition[] defs = new ColumnDefinitions.Definition[pmsg.metadata.names.size()];
                if (defs.length == 0)
                    return new PreparedStatement(new ColumnDefinitions(defs), pmsg.statementId, null, query, queryKeyspace);

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

                return new PreparedStatement(new ColumnDefinitions(defs), pmsg.statementId, allSet(pkIndexes) ? pkIndexes : null, query, queryKeyspace);
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
     * <p>
     * This method is a shortcut for {@code new BoundStatement(this).bind(...)}.
     * <p>
     * Note that while no more {@code values} than bound variables can be
     * provided, it is allowed to provide less {@code values} that there is
     * variables. In that case, the remaining variables will have to be bound
     * to values by another mean because the resulting {@code BoundStatement}
     * being executable.
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
     * If no consistency level is set through this method, the BoundStatement
     * created from this object will use the default consistency level (ONE).
     * <p>
     * Changing the default consistency level is not retroactive, it only
     * applies to BoundStatement created after the change.
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

    /**
     * The string of the query that was prepared to yield this {@code
     * PreparedStatement}.
     * <p>
     * Note that a CQL3 query may implicitely apply on the current keyspace
     * (that is, if the keyspace is not explicity qualified in the query
     * itself). For prepared queries, the current keyspace used is the one at
     * the time of the preparation, not the one at execution time. The current
     * keyspace at the time of the preparation can be retrieved through
     * {@link #getQueryKeyspace}.
     *
     * @return the query that was prepared to yield this
     * {@code PreparedStatement}.
     */
    public String getQueryString()
    {
        return query;
    }

    /**
     * The current keyspace at the time this prepared statement was prepared,
     * i.e. the one on which this statement applies unless it specificy a
     * keyspace directly.
     *
     * @return the current keyspace at the time this statement was prepared, or
     * {@code null} if no keyspace was set when the query was prepared (which
     * is possible since keyspaces can be explicitly qualified in queries and
     * so may not require a current keyspace to be set).
     */
    public String getQueryKeyspace()
    {
        return queryKeyspace;
    }

}
