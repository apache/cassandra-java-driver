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

import java.io.Closeable;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * A session holds connections to a Cassandra cluster, allowing it to be queried.
 *
 * Each session maintains multiple connections to the cluster nodes,
 * provides policies to choose which node to use for each query (round-robin on
 * all nodes of the cluster by default), and handles retries for failed query (when
 * it makes sense), etc...
 * <p>
 * Session instances are thread-safe and usually a single instance is enough
 * per application. However, a given session can only be set to one keyspace
 * at a time, so one instance per keyspace is necessary.
 */
public interface Session extends Closeable {

    /**
     * The keyspace to which this Session is currently logged in, if any.
     * <p>
     * This correspond to the name passed to {@link Cluster#connect(String)}, or to the
     * last keyspace logged into through a "USE" CQL query if one was used.
     *
     * @return the name of the keyspace to which this Session is currently
     * logged in, or {@code null} if the session is logged to no keyspace.
     */
    public String getLoggedKeyspace();

    /**
     * Force the initialization of this Session instance if it hasn't been
     * initialized yet.
     * <p>
     * Please note first that most use won't need to call this method
     * explicitly. If you use the {@link Cluster#connect} method {@code Cluster}
     * to create your Session, the returned session will be already
     * initialized. Even if you create a non-initialized session through
     * {@link Cluster#newSession}, that session will get automatically
     * initialized the first time that session is used for querying. This method
     * is thus only useful if you use {@link Cluster#newSession} and want to
     * explicitly force initialization without querying.
     * <p>
     * Session initialization consists in connecting the Session to the known
     * Cassandra hosts (at least those that should not be ignored due to
     * the {@code LoadBalancingPolicy} in place).
     * <p>
     * If the Cluster instance this Session depends on is not itself
     * initialized, it will be initialized by this method.
     * <p>
     * If the session is already initialized, this method is a no-op.
     *
     * @return this {@code Session} object.
     *
     * @throws NoHostAvailableException if this initialization triggers the
     * Cluster initialization and no host amongst the contact points can be
     * reached.
     * @throws AuthenticationException if this initialization triggers the
     * Cluster initialization and an authentication error occurs while contacting
     * the initial contact points.
     */
    public Session init();

    /**
     * Executes the provided query.
     *
     * This is a convenience method for {@code execute(new SimpleStatement(query))}.
     *
     * @param query the CQL query to execute.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     */
    public ResultSet execute(String query);

    /**
     * Executes the provided query using the provided value.
     *
     * This is a convenience method for {@code execute(new SimpleStatement(query, values))}.
     *
     * @param query the CQL query to execute.
     * @param values values required for the execution of {@code query}. See
     * {@link SimpleStatement#SimpleStatement(String, Object...)} for more detail.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     * @throws UnsupportedFeatureException if version 1 of the protocol
     * is in use (i.e. if you've force version 1 through {@link Cluster.Builder#withProtocolVersion}
     * or you use Cassandra 1.2).
     */
    public ResultSet execute(String query, Object... values);

    /**
     * Executes the provided query.
     *
     * This method blocks until at least some result has been received from the
     * database. However, for SELECT queries, it does not guarantee that the
     * result has been received in full. But it does guarantee that some
     * response has been received from the database, and in particular
     * guarantee that if the request is invalid, an exception will be thrown
     * by this method.
     *
     * @param statement the CQL query to execute (that can be any {@code Statement}).
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     * @throws UnsupportedFeatureException if the protocol version 1 is in use and
     * a feature not supported has been used. Features that are not supported by
     * the version protocol 1 include: BatchStatement, ResultSet paging and binary
     * values in RegularStatement.
     */
    public ResultSet execute(Statement statement);

    /**
     * Executes the provided query asynchronously.
     * <p>
     * This is a convenience method for {@code executeAsync(new SimpleStatement(query))}.
     *
     * @param query the CQL query to execute.
     * @return a future on the result of the query.
     */
    public ResultSetFuture executeAsync(String query);

    /**
     * Executes the provided query asynchronously using the provided values.
     *
     * This is a convenience method for {@code executeAsync(new SimpleStatement(query, values))}.
     *
     * @param query the CQL query to execute.
     * @param values values required for the execution of {@code query}. See
     * {@link SimpleStatement#SimpleStatement(String, Object...)} for more detail.
     * @return a future on the result of the query.
     *
     * @throws UnsupportedFeatureException if version 1 of the protocol
     * is in use (i.e. if you've force version 1 through {@link Cluster.Builder#withProtocolVersion}
     * or you use Cassandra 1.2).
     */
    public ResultSetFuture executeAsync(String query, Object... values);

    /**
     * Executes the provided query asynchronously.
     *
     * This method does not block. It returns as soon as the query has been
     * passed to the underlying network stack. In particular, returning from
     * this method does not guarantee that the query is valid or has even been
     * submitted to a live node. Any exception pertaining to the failure of the
     * query will be thrown when accessing the {@link ResultSetFuture}.
     * <p>
     * Note that for queries that doesn't return a result (INSERT, UPDATE and
     * DELETE), you will need to access the ResultSetFuture (that is call one of
     * its get method to make sure the query was successful.
     *
     * @param statement the CQL query to execute (that can be either any {@code Statement}.
     * @return a future on the result of the query.
     *
     * @throws UnsupportedFeatureException if the protocol version 1 is in use and
     * a feature not supported has been used. Features that are not supported by
     * the version protocol 1 include: BatchStatement, ResultSet paging and binary
     * values in RegularStatement.
     */
    public ResultSetFuture executeAsync(Statement statement);

    /**
     * Prepares the provided query string.
     *
     * @param query the CQL query string to prepare
     * @return the prepared statement corresponding to {@code query}.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to prepare this query.
     */
    public PreparedStatement prepare(String query);

    /**
     * Prepares the provided query.
     * <p>
     * This method is essentially a shortcut for {@code prepare(statement.getQueryString())},
     * but note that the resulting {@code PreparedStatement} will inherit the query properties
     * set on {@code statement}. Concretely, this means that in the following code:
     * <pre>
     *   RegularStatement toPrepare = new SimpleStatement("SELECT * FROM test WHERE k=?").setConsistencyLevel(ConsistencyLevel.QUORUM);
     *   PreparedStatement prepared = session.prepare(toPrepare);
     *   session.execute(prepared.bind("someValue"));
     * </pre>
     * the final execution will be performed with Quorum consistency.
     * <p>
     * Please note that if the same CQL statement is prepared more than once, all
     * calls to this method will return the same {@code PreparedStatement} object
     * but the method will still apply the properties of the prepared
     * {@code Statement} to this object.
     *
     * @param statement the statement to prepare
     * @return the prepared statement corresponding to {@code statement}.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to prepare this statement.
     * @throws IllegalArgumentException if {@code statement.getValues() != null}
     * (values for executing a prepared statement should be provided after preparation
     * though the {@link PreparedStatement#bind} method or through a corresponding
     * {@link BoundStatement}).
     */
    public PreparedStatement prepare(RegularStatement statement);

    /**
     * Prepares the provided query string asynchronously.
     * <p>
     * This method is equivalent to {@link #prepare(String)} except that it
     * does not block but return a future instead. Any error during preparation will
     * be thrown when accessing the future, not by this method itself.
     *
     * @param query the CQL query string to prepare
     * @return a future on the prepared statement corresponding to {@code query}.
     */
    public ListenableFuture<PreparedStatement> prepareAsync(String query);

    /**
     * Prepares the provided query asynchronously.
     * <p>
     * This method is essentially a shortcut for {@code prepareAsync(statement.getQueryString())},
     * but with the additional effect that the resulting {@code
     * PreparedStatement} will inherit the query properties set on {@code statement}.
     * <p>
     * Please note that if the same CQL statement is prepared more than once, all
     * calls to this method will return the same {@code PreparedStatement} object
     * but the method will still apply the properties of the prepared
     * {@code Statement} to this object.
     *
     * @param statement the statement to prepare
     * @return a future on the prepared statement corresponding to {@code statement}.
     *
     * @see Session#prepare(RegularStatement)
     *
     * @throws IllegalArgumentException if {@code statement.getValues() != null}
     * (values for executing a prepared statement should be provided after preparation
     * though the {@link PreparedStatement#bind} method or through a corresponding
     * {@link BoundStatement}).
     */
    public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement);

    /**
     * Initiates a shutdown of this session instance.
     * <p>
     * This method is asynchronous and return a future on the completion
     * of the shutdown process. As soon a the session is shutdown, no
     * new request will be accepted, but already submitted queries are
     * allowed to complete. This method closes all connections of this
     * session and reclaims all resources used by it.
     * <p>
     * If for some reason you wish to expedite this process, the
     * {@link CloseFuture#force} can be called on the result future.
     * <p>
     * This method has no particular effect if the session was already closed
     * (in which case the returned future will return immediately).
     * <p>
     * Note that if you want to close the full {@code Cluster} instance
     * this session is part of, you should use {@link Cluster#close} instead
     * (which will call this method for all sessions but also release some
     * additional resources).
     *
     * @return a future on the completion of the shutdown process.
     */
    public CloseFuture closeAsync();

    /**
     * Initiates a shutdown of this session instance and blocks until
     * that shutdown completes.
     * <p>
     * This method is a shortcut for {@code closeAsync().get()}.
     */
    public void close();

    /**
     * Returns the {@code Cluster} object this session is part of.
     *
     * @return the {@code Cluster} object this session is part of.
     */
    public Cluster getCluster();
}
