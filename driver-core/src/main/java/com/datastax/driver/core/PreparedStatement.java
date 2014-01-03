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

import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Represents a prepared statement, a query with bound variables that has been
 * prepared (pre-parsed) by the database.
 * <p>
 * A prepared statement can be executed once concrete values have been provided
 * for the bound variables. A prepared statement and the values for its
 * bound variables constitute a BoundStatement and can be executed (by
 * {@link Session#execute}).
 * <p>
 * A {@code PreparedStatement} object allows you to define specific defaults
 * for the different properties of a {@link Query} (Consistency level, tracing, ...),
 * in which case those properties will be inherited as default by every
 * BoundedStatement created from the {PreparedStatement}. The default for those
 * {@code PreparedStatement} properties is the same that in {@link Query} if the
 * PreparedStatement is created by {@link Session#prepare(String)} but will inherit
 * of the properties of the {@link Statement} used for the preparation if
 * {@link Session#prepare(Statement)} is used.
 */
public class PreparedStatement {

    final ColumnDefinitions metadata;
    final MD5Digest id;
    final String query;
    final String queryKeyspace;

    volatile ByteBuffer routingKey;
    final int[] routingKeyIndexes;

    volatile ConsistencyLevel consistency;
    volatile boolean traceQuery;
    volatile RetryPolicy retryPolicy;

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
                ColumnDefinitions.Definition[] defs = new ColumnDefinitions.Definition[msg.metadata.names.size()];
                if (defs.length == 0)
                    return new PreparedStatement(new ColumnDefinitions(defs), msg.statementId, null, query, queryKeyspace);

                List<ColumnMetadata> partitionKeyColumns = null;
                int[] pkIndexes = null;
                KeyspaceMetadata km = clusterMetadata.getKeyspace(msg.metadata.names.get(0).ksName);
                if (km != null) {
                    TableMetadata tm = km.getTable(msg.metadata.names.get(0).cfName);
                    if (tm != null) {
                        partitionKeyColumns = tm.getPartitionKey();
                        pkIndexes = new int[partitionKeyColumns.size()];
                        for (int i = 0; i < pkIndexes.length; ++i)
                            pkIndexes[i] = -1;
                    }
                }

                // Note: we rely on the fact CQL queries cannot span multiple tables. If that change, we'll have to get smarter.
                for (int i = 0; i < defs.length; i++) {
                    defs[i] = ColumnDefinitions.Definition.fromTransportSpecification(msg.metadata.names.get(i));
                    maybeGetIndex(defs[i].getName(), i, partitionKeyColumns, pkIndexes);
                }

                return new PreparedStatement(new ColumnDefinitions(defs), msg.statementId, allSet(pkIndexes) ? pkIndexes : null, query, queryKeyspace);
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
     * While the number of {@code values} cannot be greater than the number of bound
     * variables, the number of {@code values} may be fewer than the number of bound
     * variables. In that case, the remaining variables will have to be bound
     * to values by another mean because the resulting {@code BoundStatement}
     * being executable.
     * <p>
     * This method is a convenience for {@code new BoundStatement(this).bind(...)}.
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
     * @throws NullPointerException if one of {@code values} is a collection
     * (List, Set or Map) containing a null value. Nulls are not supported in
     * collections by CQL.
     *
     * @see BoundStatement#bind
     */
    public BoundStatement bind(Object... values) {
        BoundStatement bs = new BoundStatement(this);
        return bs.bind(values);
    }

    /**
     * Creates a new BoundStatement object for this prepared statement.
     * <p>
     * This method do not bind any values to any of the prepared variables. Said
     * values need to be bound on the resulting statement using BoundStatement's
     * setters methods ({@link BoundStatement#setInt}, {@link BoundStatement#setLong}, ...).
     *
     * @return the newly created {@code BoundStatement}.
     */
    public BoundStatement bind() {
        return new BoundStatement(this);
    }

    /**
     * Sets the routing key for this prepared statement.
     * <p>
     * While you can provide a fixed routing key for all executions of this prepared 
     * statement with this method, it is not mandatory to provide
     * one through this method. This method should only be used
     * if the partition key of the prepared query is not part of the prepared
     * variables (that is if the partition key is fixed).
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
     * Sets the routing key for this query.
     * <p>
     * See {@link #setRoutingKey(ByteBuffer)} for more information. This
     * method is a variant for when the query partition key is composite and
     * the routing key must be built from multiple values.
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
     * Sets a default consistency level for all bound statements 
     * created from this prepared statement.
     * <p>
     * If no consistency level is set through this method, the bound statement
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
     * Returns the default consistency level set through {@link #setConsistencyLevel}.
     *
     * @return the default consistency level. Returns {@code null} if no
     * consistency level has been set through this object {@code setConsistencyLevel}
     * method.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistency;
    }

    /**
     * Returns the string of the query that was prepared to yield this {@code
     * PreparedStatement}.
     * <p>
     * Note that a CQL3 query may be implicitly applied to the current keyspace
     * (that is, if the keyspace is not explicitly qualified in the query
     * itself). For prepared queries, the current keyspace used is the one at
     * the time of the preparation, not the one at execution time. The current
     * keyspace at the time of the preparation can be retrieved through
     * {@link #getQueryKeyspace}.
     *
     * @return the query that was prepared to yield this
     * {@code PreparedStatement}.
     */
    public String getQueryString() {
        return query;
    }

    /**
     * Returns the keyspace at the time that this prepared statement was prepared,
     * (that is the one on which this statement applies unless it specified a
     * keyspace explicitly).
     *
     * @return the keyspace at the time that this statement was prepared or
     * {@code null} if no keyspace was set when the query was prepared (which
     * is possible since keyspaces can be explicitly qualified in queries and
     * so may not require a current keyspace to be set).
     */
    public String getQueryKeyspace() {
        return queryKeyspace;
    }

    /**
     * Convenience method to enables tracing for all bound statements created
     * from this prepared statement.
     *
     * @return this {@code Query} object.
     */
    public PreparedStatement enableTracing() {
        this.traceQuery = true;
        return this;
    }

    /**
     * Convenience method to disable tracing for all bound statements created
     * from this prepared statement.
     *
     * @return this {@code PreparedStatement} object.
     */
    public PreparedStatement disableTracing() {
        this.traceQuery = false;
        return this;
    }

    /**
     * Returns whether tracing is enabled for this prepared statement, i.e. if
     * BoundStatement created from it will use tracing by default.
     *
     * @return {@code true} if this prepared statement has tracing enabled,
     * {@code false} otherwise.
     */
    public boolean isTracing() {
        return traceQuery;
    }

    /**
     * Convenience method to set a default retry policy for the {@code BoundStatement}
     * created from this prepared statement.
     * <p>
     * Note that this method is competely optional. By default, the retry policy
     * used is the one returned {@link com.datastax.driver.core.policies.Policies#getRetryPolicy}
     * in the cluster configuration. This method is only useful if you want
     * to override this default policy for the {@code BoundStatement} created from
     * this {@code PreparedStatement}.
     * to punctually override the default policy for this request.
     *
     * @param policy the retry policy to use for this prepared statement.
     * @return this {@code PreparedStatement} object.
     */
    public PreparedStatement setRetryPolicy(RetryPolicy policy) {
        this.retryPolicy = policy;
        return this;
    }

    /**
     * Returns the retry policy sets for this prepared statement, if any.
     *
     * @return the retry policy sets specifically for this prepared statement or
     * {@code null} if none have been set.
     */
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }
}
