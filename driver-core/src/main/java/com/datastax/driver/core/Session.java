/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.*;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

/**
 * A session holds connections to a Cassandra cluster, allowing it to be queried.
 * <p/>
 * Each session maintains multiple connections to the cluster nodes,
 * provides policies to choose which node to use for each query (round-robin on
 * all nodes of the cluster by default), and handles retries for failed queries (when
 * it makes sense), etc...
 * <p/>
 * Session instances are thread-safe and usually a single instance is enough
 * per application. As a given session can only be "logged" into one keyspace at
 * a time (where the "logged" keyspace is the one used by queries that don't
 * explicitly use a fully qualified table name), it can make sense to create one
 * session per keyspace used. This is however not necessary when querying multiple keyspaces
 * since it is always possible to use a single session with fully qualified table names
 * in queries.
 */
public interface Session extends Closeable {

    /**
     * The keyspace to which this Session is currently logged in, if any.
     * <p/>
     * This correspond to the name passed to {@link Cluster#connect(String)}, or to the
     * last keyspace logged into through a "USE" CQL query if one was used.
     *
     * @return the name of the keyspace to which this Session is currently
     * logged in, or {@code null} if the session is logged to no keyspace.
     */
    String getLoggedKeyspace();

    /**
     * Force the initialization of this Session instance if it hasn't been
     * initialized yet.
     * <p/>
     * Please note first that most users won't need to call this method
     * explicitly. If you use the {@link Cluster#connect} method
     * to create your Session, the returned session will be already
     * initialized. Even if you create a non-initialized session through
     * {@link Cluster#newSession}, that session will get automatically
     * initialized the first time it is used for querying. This method
     * is thus only useful if you use {@link Cluster#newSession} and want to
     * explicitly force initialization without querying.
     * <p/>
     * Session initialization consists in connecting the Session to the known
     * Cassandra hosts (at least those that should not be ignored due to
     * the {@link com.datastax.driver.core.policies.LoadBalancingPolicy LoadBalancingPolicy} in place).
     * <p/>
     * If the Cluster instance this Session depends on is not itself
     * initialized, it will be initialized by this method.
     * <p/>
     * If the session is already initialized, this method is a no-op.
     *
     * @return this {@code Session} object.
     * @throws NoHostAvailableException if this initialization triggers the
     *                                  {@link Cluster} initialization and no host amongst the contact points can be
     *                                  reached.
     * @throws AuthenticationException  if this initialization triggers the
     *                                  {@link Cluster} initialization and an authentication error occurs while contacting
     *                                  the initial contact points.
     */
    Session init();

    /**
     * Initialize this session asynchronously.
     *
     * @return a future that will complete when the session is fully initialized.
     * @see #init()
     */
    ListenableFuture<Session> initAsync();

    /**
     * Executes the provided query.
     * <p/>
     * This is a convenience method for {@code execute(new SimpleStatement(query))}.
     *
     * @param query the CQL query to execute.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     * @throws NoHostAvailableException if no host in the cluster can be
     *                                  contacted successfully to execute this query.
     * @throws QueryExecutionException  if the query triggered an execution
     *                                  exception, i.e. an exception thrown by Cassandra when it cannot execute
     *                                  the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     *                                  unauthorized or any other validation problem).
     */
    ResultSet execute(String query);

    /**
     * Executes the provided query using the provided values.
     * <p/>
     * This is a convenience method for {@code execute(new SimpleStatement(query, values))}.
     *
     * @param query  the CQL query to execute.
     * @param values values required for the execution of {@code query}. See
     *               {@link SimpleStatement#SimpleStatement(String, Object...)} for more details.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     * @throws NoHostAvailableException    if no host in the cluster can be
     *                                     contacted successfully to execute this query.
     * @throws QueryExecutionException     if the query triggered an execution
     *                                     exception, i.e. an exception thrown by Cassandra when it cannot execute
     *                                     the query with the requested consistency level successfully.
     * @throws QueryValidationException    if the query if invalid (syntax error,
     *                                     unauthorized or any other validation problem).
     * @throws UnsupportedFeatureException if version 1 of the protocol
     *                                     is in use (i.e. if you've forced version 1 through {@link Cluster.Builder#withProtocolVersion}
     *                                     or you use Cassandra 1.2).
     */
    ResultSet execute(String query, Object... values);

    /**
     * Executes the provided query using the provided named values.
     * <p/>
     * This is a convenience method for {@code execute(new SimpleStatement(query, values))}.
     *
     * @param query  the CQL query to execute.
     * @param values values required for the execution of {@code query}. See
     *               {@link SimpleStatement#SimpleStatement(String, Map)} for more details.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     * @throws NoHostAvailableException    if no host in the cluster can be
     *                                     contacted successfully to execute this query.
     * @throws QueryExecutionException     if the query triggered an execution
     *                                     exception, i.e. an exception thrown by Cassandra when it cannot execute
     *                                     the query with the requested consistency level successfully.
     * @throws QueryValidationException    if the query if invalid (syntax error,
     *                                     unauthorized or any other validation problem).
     * @throws UnsupportedFeatureException if version 1 or 2 of the protocol
     *                                     is in use (i.e. if you've forced it through {@link Cluster.Builder#withProtocolVersion}
     *                                     or you use Cassandra 1.2 or 2.0).
     */
    ResultSet execute(String query, Map<String, Object> values);

    /**
     * Executes the provided query.
     * <p/>
     * This method blocks until at least some result has been received from the
     * database. However, for SELECT queries, it does not guarantee that the
     * result has been received in full. But it does guarantee that some
     * response has been received from the database, and in particular
     * guarantees that if the request is invalid, an exception will be thrown
     * by this method.
     *
     * @param statement the CQL query to execute (that can be any {@link Statement}).
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     * @throws NoHostAvailableException    if no host in the cluster can be
     *                                     contacted successfully to execute this query.
     * @throws QueryExecutionException     if the query triggered an execution
     *                                     exception, i.e. an exception thrown by Cassandra when it cannot execute
     *                                     the query with the requested consistency level successfully.
     * @throws QueryValidationException    if the query if invalid (syntax error,
     *                                     unauthorized or any other validation problem).
     * @throws UnsupportedFeatureException if the protocol version 1 is in use and
     *                                     a feature not supported has been used. Features that are not supported by
     *                                     the version protocol 1 include: BatchStatement, ResultSet paging and binary
     *                                     values in RegularStatement.
     */
    ResultSet execute(Statement statement);

    /**
     * Executes the provided query asynchronously.
     * <p/>
     * This is a convenience method for {@code executeAsync(new SimpleStatement(query))}.
     *
     * @param query the CQL query to execute.
     * @return a future on the result of the query.
     */
    ResultSetFuture executeAsync(String query);

    /**
     * Executes the provided query asynchronously using the provided values.
     * <p/>
     * This is a convenience method for {@code executeAsync(new SimpleStatement(query, values))}.
     *
     * @param query  the CQL query to execute.
     * @param values values required for the execution of {@code query}. See
     *               {@link SimpleStatement#SimpleStatement(String, Object...)} for more details.
     * @return a future on the result of the query.
     * @throws UnsupportedFeatureException if version 1 of the protocol
     *                                     is in use (i.e. if you've forced version 1 through {@link Cluster.Builder#withProtocolVersion}
     *                                     or you use Cassandra 1.2).
     */
    ResultSetFuture executeAsync(String query, Object... values);

    /**
     * Executes the provided query asynchronously using the provided values.
     * <p/>
     * This is a convenience method for {@code executeAsync(new SimpleStatement(query, values))}.
     *
     * @param query  the CQL query to execute.
     * @param values values required for the execution of {@code query}. See
     *               {@link SimpleStatement#SimpleStatement(String, Map)} for more details.
     * @return a future on the result of the query.
     * @throws UnsupportedFeatureException if version 1 or 2 of the protocol
     *                                     is in use (i.e. if you've forced it through {@link Cluster.Builder#withProtocolVersion}
     *                                     or you use Cassandra 1.2 or 2.0).
     */
    ResultSetFuture executeAsync(String query, Map<String, Object> values);

    /**
     * Executes the provided query asynchronously.
     * <p/>
     * This method does not block. It returns as soon as the query has been
     * passed to the underlying network stack. In particular, returning from
     * this method does not guarantee that the query is valid or has even been
     * submitted to a live node. Any exception pertaining to the failure of the
     * query will be thrown when accessing the {@link ResultSetFuture}.
     * <p/>
     * Note that for queries that don't return a result (INSERT, UPDATE and
     * DELETE), you will need to access the ResultSetFuture (that is, call one of
     * its {@code get} methods to make sure the query was successful.
     *
     * @param statement the CQL query to execute (that can be any {@code Statement}).
     * @return a future on the result of the query.
     * @throws UnsupportedFeatureException if the protocol version 1 is in use and
     *                                     a feature not supported has been used. Features that are not supported by
     *                                     the version protocol 1 include: BatchStatement, ResultSet paging and binary
     *                                     values in RegularStatement.
     */
    ResultSetFuture executeAsync(Statement statement);

    /**
     * Prepares the provided query string.
     *
     * @param query the CQL query string to prepare
     * @return the prepared statement corresponding to {@code query}.
     * @throws NoHostAvailableException if no host in the cluster can be
     *                                  contacted successfully to prepare this query.
     */
    PreparedStatement prepare(String query);

    /**
     * Prepares the provided query.
     * <p/>
     * This method behaves like {@link #prepare(String)},
     * but note that the resulting {@code PreparedStatement} will inherit the query properties
     * set on {@code statement}. Concretely, this means that in the following code:
     * <pre>
     * RegularStatement toPrepare = new SimpleStatement("SELECT * FROM test WHERE k=?").setConsistencyLevel(ConsistencyLevel.QUORUM);
     * PreparedStatement prepared = session.prepare(toPrepare);
     * session.execute(prepared.bind("someValue"));
     * </pre>
     * the final execution will be performed with Quorum consistency.
     * <p/>
     * Please note that if the same CQL statement is prepared more than once, all
     * calls to this method will return the same {@code PreparedStatement} object
     * but the method will still apply the properties of the prepared
     * {@code Statement} to this object.
     *
     * @param statement the statement to prepare
     * @return the prepared statement corresponding to {@code statement}.
     * @throws NoHostAvailableException if no host in the cluster can be
     *                                  contacted successfully to prepare this statement.
     * @throws IllegalArgumentException if {@code statement.getValues() != null}
     *                                  (values for executing a prepared statement should be provided after preparation
     *                                  though the {@link PreparedStatement#bind} method or through a corresponding
     *                                  {@link BoundStatement}).
     */
    PreparedStatement prepare(RegularStatement statement);

    /**
     * Prepares the provided query string asynchronously.
     * <p/>
     * This method is equivalent to {@link #prepare(String)} except that it
     * does not block but return a future instead. Any error during preparation will
     * be thrown when accessing the future, not by this method itself.
     *
     * @param query the CQL query string to prepare
     * @return a future on the prepared statement corresponding to {@code query}.
     */
    ListenableFuture<PreparedStatement> prepareAsync(String query);

    /**
     * Prepares the provided query asynchronously.
     * This method behaves like {@link #prepareAsync(String)},
     * but note that the resulting {@code PreparedStatement} will inherit the query properties
     * set on {@code statement}. Concretely, this means that in the following code:
     * <pre>
     * RegularStatement toPrepare = new SimpleStatement("SELECT * FROM test WHERE k=?").setConsistencyLevel(ConsistencyLevel.QUORUM);
     * PreparedStatement prepared = session.prepare(toPrepare);
     * session.execute(prepared.bind("someValue"));
     * </pre>
     * the final execution will be performed with Quorum consistency.
     * <p/>
     * Please note that if the same CQL statement is prepared more than once, all
     * calls to this method will return the same {@code PreparedStatement} object
     * but the method will still apply the properties of the prepared
     * {@code Statement} to this object.
     *
     * @param statement the statement to prepare
     * @return a future on the prepared statement corresponding to {@code statement}.
     * @throws IllegalArgumentException if {@code statement.getValues() != null}
     *                                  (values for executing a prepared statement should be provided after preparation
     *                                  though the {@link PreparedStatement#bind} method or through a corresponding
     *                                  {@link BoundStatement}).
     * @see Session#prepare(RegularStatement)
     */
    ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement);

    /**
     * Initiates a shutdown of this session instance.
     * <p/>
     * This method is asynchronous and return a future on the completion
     * of the shutdown process. As soon as the session is shutdown, no
     * new request will be accepted, but already submitted queries are
     * allowed to complete. This method closes all connections of this
     * session and reclaims all resources used by it.
     * <p/>
     * If for some reason you wish to expedite this process, the
     * {@link CloseFuture#force} can be called on the result future.
     * <p/>
     * This method has no particular effect if the session was already closed
     * (in which case the returned future will return immediately).
     * <p/>
     * Note that this method does not close the corresponding {@code Cluster}
     * instance (which holds additional resources, in particular internal
     * executors that must be shut down in order for the client program to
     * terminate).
     * If you want to do so, use {@link Cluster#close}, but note that it will
     * close all sessions created from that cluster.
     *
     * @return a future on the completion of the shutdown process.
     */
    CloseFuture closeAsync();

    /**
     * Initiates a shutdown of this session instance and blocks until
     * that shutdown completes.
     * <p/>
     * This method is a shortcut for {@code closeAsync().get()}.
     * <p/>
     * Note that this method does not close the corresponding {@code Cluster}
     * instance (which holds additional resources, in particular internal
     * executors that must be shut down in order for the client program to
     * terminate).
     * If you want to do so, use {@link Cluster#close}, but note that it will
     * close all sessions created from that cluster.
     */
    @Override
    void close();

    /**
     * Whether this Session instance has been closed.
     * <p/>
     * Note that this method returns true as soon as the closing of this Session
     * has started but it does not guarantee that the closing is done. If you
     * want to guarantee that the closing is done, you can call {@code close()}
     * and wait until it returns (or call the get method on {@code closeAsync()}
     * with a very short timeout and check this doesn't timeout).
     *
     * @return {@code true} if this Session instance has been closed, {@code false}
     * otherwise.
     */
    boolean isClosed();

    /**
     * Returns the {@code Cluster} object this session is part of.
     *
     * @return the {@code Cluster} object this session is part of.
     */
    Cluster getCluster();

    /**
     * Return a snapshot of the state of this Session.
     * <p/>
     * The returned object provides information on which hosts the session is
     * connected to, how many connections are opened to each host, etc...
     * The returned object is immutable, it is a snapshot of the Session State
     * taken when this method is called.
     *
     * @return a snapshot of the state of this Session.
     */
    State getState();

    /**
     * The state of a Session.
     * <p/>
     * This mostly exposes information on the connections maintained by a Session:
     * which host it is connected to, how many connections it has for each host, etc...
     */
    interface State {
        /**
         * The Session to which this State corresponds to.
         *
         * @return the Session to which this State corresponds to.
         */
        Session getSession();

        /**
         * The hosts to which the session is currently connected (more precisely, at the time
         * this State has been grabbed).
         * <p/>
         * Please note that this method really returns the hosts for which the session currently
         * holds a connection pool. As such, it's unlikely but not impossible for a host to be listed
         * in the output of this method but to have {@code getOpenConnections} return 0, if the
         * pool itself is created but no connections have been successfully opened yet.
         *
         * @return an immutable collection of the hosts to which the session is connected.
         */
        Collection<Host> getConnectedHosts();

        /**
         * The number of open connections to a given host.
         * <p/>
         * Note that this refers to <em>active</em> connections. The actual number of connections also
         * includes {@link #getTrashedConnections(Host)}.
         *
         * @param host the host to get open connections for.
         * @return The number of open connections to {@code host}. If the session
         * is not connected to that host, 0 is returned.
         */
        int getOpenConnections(Host host);

        /**
         * The number of "trashed" connections to a given host.
         * <p/>
         * When the load to a host decreases, the driver will reclaim some connections in order to save
         * resources. No requests are sent to these connections anymore, but they are kept open for an
         * additional amount of time ({@link PoolingOptions#getIdleTimeoutSeconds()}), in case the load
         * goes up again. This method counts connections in that state.
         *
         * @param host the host to get trashed connections for.
         * @return The number of trashed connections to {@code host}. If the session
         * is not connected to that host, 0 is returned.
         */
        int getTrashedConnections(Host host);

        /**
         * The number of queries that are currently being executed through a given host.
         * <p/>
         * This corresponds to the number of queries that have been sent (by the session this
         * is a State of) to the Cassandra Host on one of its connections but haven't yet returned.
         * In that sense this provides a sort of measure of how busy the connections to that node
         * are (at the time the {@code State} was grabbed at least).
         *
         * @param host the host to get in-flight queries for.
         * @return the number of currently (as in 'at the time the state was grabbed') executing
         * queries to {@code host}.
         */
        int getInFlightQueries(Host host);
    }
}
