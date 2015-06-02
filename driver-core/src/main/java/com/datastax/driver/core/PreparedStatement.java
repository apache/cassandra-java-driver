/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import com.datastax.driver.core.exceptions.InvalidTypeException;
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
 * for the different properties of a {@link Statement} (Consistency level, tracing, ...),
 * in which case those properties will be inherited as default by every
 * BoundedStatement created from the {PreparedStatement}. The default for those
 * {@code PreparedStatement} properties is the same that in {@link Statement} if the
 * PreparedStatement is created by {@link Session#prepare(String)} but will inherit
 * of the properties of the {@link RegularStatement} used for the preparation if
 * {@link Session#prepare(RegularStatement)} is used.
 */
public interface PreparedStatement {

    /**
     * Returns metadata on the bounded variables of this prepared statement.
     *
     * @return the variables bounded in this prepared statement.
     */
    public ColumnDefinitions getVariables();

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
    public BoundStatement bind(Object... values);

    /**
     * Creates a new BoundStatement object for this prepared statement.
     * <p>
     * This method do not bind any values to any of the prepared variables. Said
     * values need to be bound on the resulting statement using BoundStatement's
     * setters methods ({@link BoundStatement#setInt}, {@link BoundStatement#setLong}, ...).
     *
     * @return the newly created {@code BoundStatement}.
     */
    public BoundStatement bind();

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
     * <p>
     * If the partition key is neither fixed nor part of the prepared variables (e.g.
     * a composite partition key where only some of the components are bound), the
     * routing key can also be set on each bound statement.
     *
     * @param routingKey the raw (binary) value to use as routing key.
     * @return this {@code PreparedStatement} object.
     *
     * @see Statement#getRoutingKey
     * @see BoundStatement#getRoutingKey
     */
    public PreparedStatement setRoutingKey(ByteBuffer routingKey);

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
     * @see Statement#getRoutingKey
     */
    public PreparedStatement setRoutingKey(ByteBuffer... routingKeyComponents);

    /**
     * Returns the routing key set for this query.
     *
     * @return the routing key for this query or {@code null} if none has been
     * explicitly set on this PreparedStatement.
     */
    public ByteBuffer getRoutingKey();

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
    public PreparedStatement setConsistencyLevel(ConsistencyLevel consistency);

    /**
     * Returns the default consistency level set through {@link #setConsistencyLevel}.
     *
     * @return the default consistency level. Returns {@code null} if no
     * consistency level has been set through this object {@code setConsistencyLevel}
     * method.
     */
    public ConsistencyLevel getConsistencyLevel();

    /**
     * Sets a default serial consistency level for all bound statements
     * created from this prepared statement.
     * <p>
     * If no serial consistency level is set through this method, the bound statement
     * created from this object will use the default serial consistency level (SERIAL).
     * <p>
     * Changing the default serial consistency level is not retroactive, it only
     * applies to BoundStatement created after the change.
     *
     * @param serialConsistency the default serial consistency level to set.
     * @return this {@code PreparedStatement} object.
     *
     * @throws IllegalArgumentException if {@code serialConsistency} is not one of
     * {@code ConsistencyLevel.SERIAL} or {@code ConsistencyLevel.LOCAL_SERIAL}.
     */
    public PreparedStatement setSerialConsistencyLevel(ConsistencyLevel serialConsistency);

    /**
     * Returns the default serial consistency level set through {@link #setSerialConsistencyLevel}.
     *
     * @return the default serial consistency level. Returns {@code null} if no
     * consistency level has been set through this object {@code setSerialConsistencyLevel}
     * method.
     */
    public ConsistencyLevel getSerialConsistencyLevel();

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
    public String getQueryString();

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
    public String getQueryKeyspace();

    /**
     * Convenience method to enables tracing for all bound statements created
     * from this prepared statement.
     *
     * @return this {@code Query} object.
     */
    public PreparedStatement enableTracing();

    /**
     * Convenience method to disable tracing for all bound statements created
     * from this prepared statement.
     *
     * @return this {@code PreparedStatement} object.
     */
    public PreparedStatement disableTracing();

    /**
     * Returns whether tracing is enabled for this prepared statement, i.e. if
     * BoundStatement created from it will use tracing by default.
     *
     * @return {@code true} if this prepared statement has tracing enabled,
     * {@code false} otherwise.
     */
    public boolean isTracing();

    /**
     * Convenience method to set a default retry policy for the {@code BoundStatement}
     * created from this prepared statement.
     * <p>
     * Note that this method is completely optional. By default, the retry policy
     * used is the one returned {@link com.datastax.driver.core.policies.Policies#getRetryPolicy}
     * in the cluster configuration. This method is only useful if you want
     * to override this default policy for the {@code BoundStatement} created from
     * this {@code PreparedStatement}.
     * to punctually override the default policy for this request.
     *
     * @param policy the retry policy to use for this prepared statement.
     * @return this {@code PreparedStatement} object.
     */
    public PreparedStatement setRetryPolicy(RetryPolicy policy);

    /**
     * Returns the retry policy sets for this prepared statement, if any.
     *
     * @return the retry policy sets specifically for this prepared statement or
     * {@code null} if none have been set.
     */
    public RetryPolicy getRetryPolicy();

    /**
     * Returns the prepared Id for this statement.
     *
     * @return the PreparedId corresponding to this statement.
     */
    public PreparedId getPreparedId();
}
