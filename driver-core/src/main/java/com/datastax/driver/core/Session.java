package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.configuration.*;

import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A session holds connections to a Cassandra cluster, allowing to query it.
 *
 * Each session will maintain multiple connections to the cluster nodes, and
 * provides policies to choose which node to use for each query (round-robin on
 * all nodes of the cluster by default), handles retries for failed query (when
 * it makes sense), etc...
 * <p>
 * Session instances are thread-safe and usually a single instance is enough
 * per application. However, a given session can only be set to one keyspace
 * at a time, so one instance per keyspace is necessary.
 */
public class Session {

    private static final Logger logger = LoggerFactory.getLogger(Session.class);

    final Manager manager;

    // Package protected, only Cluster should construct that.
    Session(Cluster cluster, Collection<Host> hosts) {
        this.manager = new Manager(cluster, hosts);
    }

    /**
     * Execute the provided query.
     *
     * This method is a shortcut for {@code execute(query, null)}.
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
    public ResultSet execute(String query) throws NoHostAvailableException {
        return execute(query, QueryOptions.DEFAULT);
    }

    /**
     * Execute the provided query.
     *
     * This method is a shortcut for {@code execute(query, new QueryOptions(consistency))}.
     *
     * @param query the CQL query to execute.
     * @param consistency the consistency level for the operation. If the query
     * doesn't need a consistency level (USE, CREATE, ALTER, DROP and TRUNCATE
     * queries for instance), this argument is ignored and null can be
     * provided. However, if null is provided while the query requires a
     * consistency level, the default consistency level of ONE is used.
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
    public ResultSet execute(String query, ConsistencyLevel consistency) throws NoHostAvailableException {
        return execute(query, new QueryOptions(consistency));
    }

    /**
     * Execute the provided query.
     *
     * This method blocks until at least some result has been received from the
     * database. However, for SELECT queries, it does not guarantee that the
     * result has been received in full. But it does guarantee that some
     * response has been received from the database, and in particular
     * guarantee that if the request is invalid, an exception will be thrown
     * by this method.
     *
     * @param query the CQL query to execute.
     * @param queryOptions the options to use for this query. This includes at
     * least the consistency level for the operation.
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
    public ResultSet execute(String query, QueryOptions options) throws NoHostAvailableException {
        return executeAsync(query, options).getUninterruptibly();
    }

    /**
     * Execute the provided query asynchronously.
     *
     * This method is a shortcut for {@code executeAsync(query, null)}.
     *
     * @param query the CQL query to execute.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     */
    public ResultSet.Future executeAsync(String query) {
        return executeAsync(query, QueryOptions.DEFAULT);
    }

    /**
     * Execute the provided query asynchronously.
     *
     * This method is a shortcut for {@code executeAsync(query, new QueryOptions(consistency))}.
     *
     * @param query the CQL query to execute.
     * @param consistency the consistency level for the operation. If the query
     * doesn't need a consistency level (USE, CREATE, ALTER, DROP and TRUNCATE
     * queries for instance), this argument is ignored and null can be
     * provided. However, if null is provided while the query requires a
     * consistency level, the default consistency level of ONE is used.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     */
    public ResultSet.Future executeAsync(String query, ConsistencyLevel consistency) {
        return executeAsync(query, new QueryOptions(consistency));
    }

    /**
     * Execute the provided query asynchronously.
     *
     * This method does not block. It returns as soon as the query has been
     * successfully sent to a Cassandra node. In particular, returning from
     * this method does not guarantee that the query is valid. Any exception
     * pertaining to the failure of the query will be thrown by the first
     * access to the {@link ResultSet}.
     *
     * Note that for queries that doesn't return a result (INSERT, UPDATE and
     * DELETE), you will need to access the ResultSet (i.e. call any of its
     * method) to make sure the query was successful.
     *
     * @param query the CQL query to execute.
     * @param queryOptions the options to use for this query. This includes at
     * least the consistency level for the operation.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     */
    public ResultSet.Future executeAsync(String query, QueryOptions options) {
        return manager.executeQuery(new QueryMessage(query, ConsistencyLevel.toCassandraCL(options.getConsistencyLevel())), options);
    }

    /**
     * Prepare the provided query.
     *
     * @param query the CQL query to prepare
     * @return the prepared statement corresponding to {@code query}.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     */
    public PreparedStatement prepare(String query) throws NoHostAvailableException {
        Connection.Future future = new Connection.Future(new PrepareMessage(query));
        manager.execute(future, QueryOptions.DEFAULT);
        return toPreparedStatement(query, future);
    }

    /**
     * Execute a prepared statement that had values provided for its bound
     * variables.
     *
     * This method is a shortcut for {@code executePrepared(stmt, null)}.
     *
     * @param stmt the prepared statement with values for its bound variables.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws IllegalStateException if {@code !stmt.ready()}.
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     */
    public ResultSet executePrepared(BoundStatement stmt) throws NoHostAvailableException {
        return executePrepared(stmt, new QueryOptions());
    }

    /**
     * Execute a prepared statement that had values provided for its bound
     * variables.
     *
     * This method is a shortcut for {@code executePrepared(stmt, new QueryOptions(consistency))}.
     *
     * @param stmt the prepared statement with values for its bound variables.
     * @param consistency the consistency level for the operation. If the query
     * doesn't need a consistency level (USE, CREATE, ALTER, DROP and TRUNCATE
     * queries for instance), this argument is ignored and null can be
     * provided. However, if null is provided while the query requires a
     * consistency level, the default consistency level of ONE is used.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws IllegalStateException if {@code !stmt.ready()}.
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     */
    public ResultSet executePrepared(BoundStatement stmt, ConsistencyLevel consistency) throws NoHostAvailableException {
        return executePrepared(stmt, new QueryOptions(consistency));
    }

    /**
     * Execute a prepared statement that had values provided for its bound
     * variables.
     *
     * This method performs like {@link #execute} but for prepared statements.
     * It blocks until at least some result has been received from the
     * database.
     *
     * @param stmt the prepared statement with values for its bound variables.
     * @param queryOptions the options to use for this query. This includes at
     * least the consistency level for the operation.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws IllegalStateException if {@code !stmt.ready()}.
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     */
    public ResultSet executePrepared(BoundStatement stmt, QueryOptions options) throws NoHostAvailableException {
        return executePreparedAsync(stmt, options).getUninterruptibly();
    }

    /**
     * Execute a prepared statement that had values provided for its bound
     * variables asynchronously.
     *
     * This method is a shortcut for {@code executePreparedAsync(stmt, null)}.
     *
     * @param stmt the prepared statement with values for its bound variables.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws IllegalStateException if {@code !stmt.ready()}.
     */
    public ResultSet.Future executePreparedAsync(BoundStatement stmt) {
        return executePreparedAsync(stmt, new QueryOptions());
    }

    /**
     * Execute a prepared statement that had values provided for its bound
     * variables asynchronously.
     *
     * This method is a shortcut for {@code executePreparedAsync(stmt, new QueryOptions(consistency))}.
     *
     * @param stmt the prepared statement with values for its bound variables.
     * @param consistency the consistency level for the operation. If the query
     * doesn't need a consistency level (USE, CREATE, ALTER, DROP and TRUNCATE
     * queries for instance), this argument is ignored and null can be
     * provided. However, if null is provided while the query requires a
     * consistency level, the default consistency level of ONE is used.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws IllegalStateException if {@code !stmt.ready()}.
     */
    public ResultSet.Future executePreparedAsync(BoundStatement stmt, ConsistencyLevel consistency) {
        return executePreparedAsync(stmt, new QueryOptions(consistency));
    }

    /**
     * Execute a prepared statement that had values provided for its bound
     * variables asynchronously.
     * <p>
     * This method performs like {@link #executeAsync} but for prepared
     * statements. It return as soon as the query has been successfully sent to
     * the database.
     *
     * @param stmt the prepared statement with values for its bound variables.
     * @param queryOptions the options to use for this query. This includes at
     * least the consistency level for the operation.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws IllegalStateException if {@code !stmt.ready()}.
     */
    public ResultSet.Future executePreparedAsync(BoundStatement stmt, QueryOptions queryOptions) {
        if (!stmt.isReady())
            throw new IllegalStateException("Some bind variables haven't been bound in the provided statement");

        return manager.executeQuery(new ExecuteMessage(stmt.statement.id, Arrays.asList(stmt.values), ConsistencyLevel.toCassandraCL(queryOptions.getConsistencyLevel())), queryOptions);
    }

    /**
     * Shutdown this session instance.
     * <p>
     * This closes all connections used by this sessions. Note that if you want
     * to shutdown the full {@code Cluster} instance this session is part of,
     * you should use {@link Cluster#shutdown} instead (which will call this
     * method for all session but also release some additional ressources).
     * <p>
     * This method has no effect if the session was already shutdown.
     */
    public void shutdown() {
        manager.shutdown();
    }

    /**
     * The {@code Cluster} object this session is part of.
     *
     * @return the {@code Cluster} object this session is part of.
     */
    public Cluster getCluster() {
        return manager.cluster;
    }

    private PreparedStatement toPreparedStatement(String query, Connection.Future future) throws NoHostAvailableException {

        try {
            Message.Response response = null;
            try {
                while (response == null) {
                    try {
                        response = future.get();
                    } catch (InterruptedException e) {
                        // TODO: decide wether we want to expose Interrupted exceptions or not
                    }
                }
            } catch (ExecutionException e) {
                ResultSet.Future.extractCauseFromExecutionException(e);
                throw new AssertionError();
            }

            assert response != null;
            switch (response.type) {
                case RESULT:
                    ResultMessage rm = (ResultMessage)response;
                    switch (rm.kind) {
                        case PREPARED:
                            ResultMessage.Prepared pmsg = (ResultMessage.Prepared)rm;
                            manager.cluster.manager.prepare(pmsg.statementId, query, future.getAddress());
                            return PreparedStatement.fromMessage(pmsg);
                        default:
                            throw new DriverInternalError(String.format("%s response received when prepared statement was expected", rm.kind));
                    }
                case ERROR:
                    ResultSet.Future.extractCause(ResultSet.Future.convertException(((ErrorMessage)response).error));
                    break;
                default:
                    throw new DriverInternalError(String.format("%s response received when prepared statement was expected", response.type));
            }
            throw new AssertionError();
        } catch (QueryExecutionException e) {
            // Preparing a statement cannot throw any of the QueryExecutionException
            throw new DriverInternalError("Received unexpected QueryExecutionException while preparing statement", e);
        }
    }

    static class Manager implements Host.StateListener {

        final Cluster cluster;

        final ConcurrentMap<Host, HostConnectionPool> pools;
        final LoadBalancingPolicy loadBalancer;

        final HostConnectionPool.PoolState poolsState;

        final AtomicBoolean isShutdown = new AtomicBoolean(false);

        public Connection.Factory connectionFactory() {
            return cluster.manager.connectionFactory;
        }

        public Cluster.Configuration configuration() {
            return cluster.manager.configuration;
        }

        public ExecutorService executor() {
            return cluster.manager.executor;
        }

        public Manager(Cluster cluster, Collection<Host> hosts) {
            this.cluster = cluster;

            // TODO: consider the use of NonBlockingHashMap
            this.pools = new ConcurrentHashMap<Host, HostConnectionPool>(hosts.size());
            this.loadBalancer = cluster.manager.configuration.getPolicies().getLoadBalancingPolicyFactory().create(hosts);
            this.poolsState = new HostConnectionPool.PoolState();

            for (Host host : hosts)
                addHost(host);
        }

        private void shutdown() {

            if (!isShutdown.compareAndSet(false, true))
                return;

            for (HostConnectionPool pool : pools.values())
                pool.shutdown();
        }

        private HostConnectionPool addHost(Host host) {
            try {
                HostDistance distance = loadBalancer.distance(host);
                if (distance == HostDistance.IGNORED) {
                    return pools.get(host);
                } else {
                    logger.debug("Adding {} to list of queried hosts", host);
                    return pools.put(host, new HostConnectionPool(host, distance, this));
                }
            } catch (AuthenticationException e) {
                logger.error("Error creating pool to {} ({})", host, e.getMessage());
                host.getMonitor().signalConnectionFailure(new ConnectionException(e.getHost(), e.getMessage()));
                return pools.get(host);
            } catch (ConnectionException e) {
                logger.debug("Error creating pool to {} ({})", host, e.getMessage());
                host.getMonitor().signalConnectionFailure(e);
                return pools.get(host);
            }
        }

        public void onUp(Host host) {
            HostConnectionPool previous = addHost(host);;
            loadBalancer.onUp(host);

            // This should not be necessary but it's harmless
            if (previous != null)
                previous.shutdown();
        }

        public void onDown(Host host) {
            loadBalancer.onDown(host);
            HostConnectionPool pool = pools.remove(host);

            // This should not be necessary but it's harmless
            if (pool != null)
                pool.shutdown();

            // If we've remove a host, the loadBalancer is allowed to change his mind on host distances.
            for (Host h : cluster.getMetadata().allHosts()) {
                if (!h.getMonitor().isUp())
                    continue;

                HostDistance dist = loadBalancer.distance(h);
                if (dist != HostDistance.IGNORED) {
                    HostConnectionPool p = pools.get(h);
                    if (p == null)
                        addHost(host);
                    else
                        p.hostDistance = dist;
                }
            }
        }

        public void onAdd(Host host) {
            HostConnectionPool previous = addHost(host);;
            loadBalancer.onAdd(host);

            // This should not be necessary, especially since the host is
            // supposed to be new, but it's safer to make that work correctly
            // if the even is triggered multiple times.
            if (previous != null)
                previous.shutdown();
        }

        public void onRemove(Host host) {
            loadBalancer.onRemove(host);
            HostConnectionPool pool = pools.remove(host);
            if (pool != null)
                pool.shutdown();
        }

        public void setKeyspace(String keyspace) throws NoHostAvailableException {
            try {
                executeQuery(new QueryMessage("use " + keyspace, ConsistencyLevel.DEFAULT_CASSANDRA_CL), new QueryOptions()).get();
            } catch (InterruptedException e) {
                // TODO: do we want to handle interrupted exception in a better way?
                throw new DriverInternalError("Hey! I was waiting!", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                // A USE query should never fail unless we cannot contact a node
                if (cause instanceof NoHostAvailableException)
                    throw (NoHostAvailableException)cause;
                else if (cause instanceof DriverUncheckedException)
                    throw (DriverUncheckedException)cause;
                else
                    throw new DriverInternalError("Unexpected exception thrown", cause);
            }
        }

        /**
         * Execute the provided request.
         *
         * This method will find a suitable node to connect to using the
         * {@link LoadBalancingPolicy} and handle host failover.
         */
        public void execute(Connection.ResponseCallback callback, QueryOptions options) {
            new RetryingCallback(this, callback, options).sendRequest();
        }

        public void prepare(String query, InetSocketAddress toExclude) {
            for (Map.Entry<Host, HostConnectionPool> entry : pools.entrySet()) {
                if (entry.getKey().getAddress().equals(toExclude))
                    continue;

                // Let's not wait too long if we can't get a connection. Things
                // will fix themselves once the user tries a query anyway.
                Connection c = null;
                try {
                    c = entry.getValue().borrowConnection(200, TimeUnit.MILLISECONDS);
                    c.write(new PrepareMessage(query)).get();
                } catch (ConnectionException e) {
                    // Again, not being able to prepare the query right now is no big deal, so just ignore
                } catch (BusyConnectionException e) {
                    // Same as above
                } catch (TimeoutException e) {
                    // Same as above
                } catch (InterruptedException e) {
                    // Same as above
                } catch (ExecutionException e) {
                    // We shouldn't really get exception while preparing a
                    // query, so log this (but ignore otherwise as it's not a big deal)
                    logger.error(String.format("Unexpected error while preparing query (%s) on %s", query, entry.getKey()), e);
                } finally {
                    if (c != null)
                        entry.getValue().returnConnection(c);
                }
            }
        }

        public ResultSet.Future executeQuery(Message.Request msg, QueryOptions options) {
            if (options.isTracing())
                msg.setTracingRequested();

            ResultSet.Future future = new ResultSet.Future(this, msg);
            execute(future.callback, options);
            return future;
        }
    }
}
