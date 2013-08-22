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

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.*;

import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class Session {

    private static final Logger logger = LoggerFactory.getLogger(Session.class);

    final Manager manager;

    // Package protected, only Cluster should construct that.
    Session(Cluster cluster, Collection<Host> hosts) {
        this.manager = new Manager(cluster, hosts);
    }

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
    public ResultSet execute(String query) {
        return execute(new SimpleStatement(query));
    }

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
     * @param query the CQL query to execute (that can be either a {@code
     * Statement} or a {@code BoundStatement}). If it is a {@code
     * BoundStatement}, all variables must have been bound (the statement must
     * be ready).
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
     * @throws IllegalStateException if {@code query} is a {@code BoundStatement}
     * but {@code !query.isReady()}.
     */
    public ResultSet execute(Query query) {
        return executeAsync(query).getUninterruptibly();
    }

    /**
     * Executes the provided query asynchronously.
     *
     * This is a convenience method for {@code executeAsync(new SimpleStatement(query))}.
     *
     * @param query the CQL query to execute.
     * @return a future on the result of the query.
     */
    public ResultSetFuture executeAsync(String query) {
        return executeAsync(new SimpleStatement(query));
    }

    /**
     * Executes the provided query asynchronously.
     *
     * This method does not block. It returns as soon as the query has been
     * passed to the underlying network stack. In particular, returning from
     * this method does not guarantee that the query is valid or has even been
     * submitted to a live node. Any exception pertaining to the failure of the
     * query will be thrown when accessing the {@link ResultSetFuture}.
     *
     * Note that for queries that doesn't return a result (INSERT, UPDATE and
     * DELETE), you will need to access the ResultSetFuture (that is call one of
     * its get method to make sure the query was successful.
     *
     * @param query the CQL query to execute (that can be either a {@code
     * Statement} or a {@code BoundStatement}). If it is a {@code
     * BoundStatement}, all variables must have been bound (the statement must
     * be ready).
     * @return a future on the result of the query.
     *
     * @throws IllegalStateException if {@code query} is a {@code BoundStatement}
     * but {@code !query.isReady()}.
     */
    public ResultSetFuture executeAsync(Query query) {

        if (query instanceof Statement) {
            return manager.executeQuery(new QueryMessage(((Statement)query).getQueryString(), ConsistencyLevel.toCassandraCL(query.getConsistencyLevel())), query);
        } else {
            assert query instanceof BoundStatement : query;

            BoundStatement bs = (BoundStatement)query;
            return manager.executeQuery(new ExecuteMessage(bs.statement.id, Arrays.asList(bs.values), ConsistencyLevel.toCassandraCL(query.getConsistencyLevel())), query);
        }
    }

    /**
     * Prepares the provided query string.
     *
     * @param query the CQL query string to prepare
     * @return the prepared statement corresponding to {@code query}.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to prepare this query.
     */
    public PreparedStatement prepare(String query) {
        Connection.Future future = new Connection.Future(new PrepareMessage(query));
        manager.execute(future, Query.DEFAULT);
        return toPreparedStatement(query, future);
    }

    /**
     * Prepares the provided query.
     * <p>
     * This method is essentially a shortcut for {@code prepare(statement.getQueryString())},
     * but note that the resulting {@code PreparedStamenent} will inherit the query properties
     * set on {@code statement}. Concretely, this means that in the following code:
     * <pre>
     *   Statement toPrepare = new SimpleStatement("SELECT * FROM test WHERE k=?").setConsistencyLevel(ConsistencyLevel.QUORUM);
     *   PreparedStatement prepared = session.prepare(toPrepare);
     *   session.execute(prepared.bind("someValue"));
     * </pre>
     * the final execution will be performed with Quorum consistency.
     *
     * @param statement the statement to prepare
     * @return the prepared statement corresponding to {@code statement}.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to prepare this statement.
     */
    public PreparedStatement prepare(Statement statement) {
        PreparedStatement prepared = prepare(statement.getQueryString());

        prepared.setConsistencyLevel(statement.getConsistencyLevel());
        if (statement.isTracing())
            prepared.enableTracing();
        prepared.setRetryPolicy(statement.getRetryPolicy());

        return prepared;
    }

    /**
     * Shuts down this session instance.
     * <p>
     * This closes all connections used by this sessions. Note that if you want
     * to shut down the full {@code Cluster} instance this session is part of,
     * you should use {@link Cluster#shutdown} instead (which will call this
     * method for all session but also release some additional resources).
     * <p>
     * This method has no effect if the session was already shutdown.
     */
    public void shutdown() {
        shutdown(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * Shutdown this session instance, only waiting a definite amount of time.
     * <p>
     * This closes all connections used by this sessions. Note that if you want
     * to shutdown the full {@code Cluster} instance this session is part of,
     * you should use {@link Cluster#shutdown} instead (which will call this
     * method for all session but also release some additional resources).
     * <p>
     * Note that this method is not thread safe in the sense that if another
     * shutdown is perform in parallel, it might return {@code true} even if
     * the instance is not yet fully shutdown.
     *
     * @param timeout how long to wait for the session to shutdown.
     * @param unit the unit for the timeout.
     * @return {@code true} if the session has been properly shutdown within
     * the {@code timeout}, {@code false} otherwise.
     */
    public boolean shutdown(long timeout, TimeUnit unit) {
        try {
            return manager.shutdown(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Returns the {@code Cluster} object this session is part of.
     *
     * @return the {@code Cluster} object this session is part of.
     */
    public Cluster getCluster() {
        return manager.cluster;
    }

    private PreparedStatement toPreparedStatement(String query, Connection.Future future) {

        try {
            Message.Response response = Uninterruptibles.getUninterruptibly(future);
            switch (response.type) {
                case RESULT:
                    ResultMessage rm = (ResultMessage)response;
                    switch (rm.kind) {
                        case PREPARED:
                            ResultMessage.Prepared pmsg = (ResultMessage.Prepared)rm;
                            PreparedStatement stmt = PreparedStatement.fromMessage(pmsg, manager.cluster.getMetadata(), query, manager.poolsState.keyspace);
                            try {
                                manager.cluster.manager.prepare(pmsg.statementId, stmt, future.getAddress());
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                // This method doesn't propagate interruption, at least not for now. However, if we've
                                // interrupted preparing queries on other node it's not a problem as we'll re-prepare
                                // later if need be. So just ignore.
                            }
                            return stmt;
                        default:
                            throw new DriverInternalError(String.format("%s response received when prepared statement was expected", rm.kind));
                    }
                case ERROR:
                    ResultSetFuture.extractCause(ResultSetFuture.convertException(((ErrorMessage)response).error));
                    break;
                default:
                    throw new DriverInternalError(String.format("%s response received when prepared statement was expected", response.type));
            }
            throw new AssertionError();
        } catch (ExecutionException e) {
            ResultSetFuture.extractCauseFromExecutionException(e);
            throw new AssertionError();
        }
    }

    static class Manager implements Host.StateListener {

        final Cluster cluster;

        final ConcurrentMap<Host, HostConnectionPool> pools;

        final HostConnectionPool.PoolState poolsState;

        final AtomicBoolean isShutdown = new AtomicBoolean(false);

        public Manager(Cluster cluster, Collection<Host> hosts) {
            this.cluster = cluster;

            this.pools = new ConcurrentHashMap<Host, HostConnectionPool>(hosts.size());
            this.poolsState = new HostConnectionPool.PoolState();

            // Create pool to initial nodes (and wait for them to be created)
            for (Host host : hosts)
            {
                try
                {
                    addOrRenewPool(host).get();
                }
                catch (ExecutionException e)
                {
                    // This is not supposed to happen
                    throw new DriverInternalError(e);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public Connection.Factory connectionFactory() {
            return cluster.manager.connectionFactory;
        }

        public Configuration configuration() {
            return cluster.manager.configuration;
        }

        LoadBalancingPolicy loadBalancingPolicy() {
            return cluster.manager.loadBalancingPolicy();
        }

        ReconnectionPolicy reconnectionPolicy() {
            return cluster.manager.reconnectionPolicy();
        }

        public ListeningExecutorService executor() {
            return cluster.manager.executor;
        }

        private boolean shutdown(long timeout, TimeUnit unit) throws InterruptedException {

            if (!isShutdown.compareAndSet(false, true))
                return true;

            long start = System.nanoTime();
            boolean success = true;
            for (HostConnectionPool pool : pools.values())
                success &= pool.shutdown(timeout - Cluster.timeSince(start, unit), unit);
            return success;
        }

        private ListenableFuture<?> addOrRenewPool(final Host host) {
            final HostDistance distance = cluster.manager.loadBalancingPolicy().distance(host);
            if (distance == HostDistance.IGNORED)
                return Futures.immediateFuture(null);

            // Creating a pool is somewhat long since it has to create the connection, so do it asynchronously.
            return executor().submit(new Runnable() {
                public void run() {
                    logger.debug("Adding {} to list of queried hosts", host);
                    try {
                        HostConnectionPool previous = pools.put(host, new HostConnectionPool(host, distance, Session.Manager.this));
                        if (previous != null)
                            previous.shutdown(); // The previous was probably already shutdown but that's ok
                    } catch (AuthenticationException e) {
                        logger.error("Error creating pool to {} ({})", host, e.getMessage());
                        cluster.manager.signalConnectionFailure(host, new ConnectionException(e.getHost(), e.getMessage()));
                    } catch (ConnectionException e) {
                        logger.debug("Error creating pool to {} ({})", host, e.getMessage());
                        cluster.manager.signalConnectionFailure(host, e);
                    }
                }
            });
        }

        ListenableFuture<?> removePool(Host host) {
            final HostConnectionPool pool = pools.remove(host);
            if (pool == null)
                return Futures.immediateFuture(null);

            // Shutdown can take some time and we don't care about holding the thread on that.
            return executor().submit(new Runnable() {
                public void run() {
                    pool.shutdown();
                }
            });
        }

        /*
         * When the set of live nodes change, the loadbalancer will change his
         * mind on host distances. It might change it on the node that came/left
         * but also on other nodes (for instance, if a node dies, another
         * previously ignored node may be now considered).
         *
         * This method ensures that all hosts for which a pool should exist
         * have one, and hosts that shouldn't don't.
         */
        private void updateCreatedPools() {
            for (Host h : cluster.getMetadata().allHosts()) {
                HostDistance dist = loadBalancingPolicy().distance(h);
                HostConnectionPool pool = pools.get(h);

                if (pool == null) {
                    if (dist != HostDistance.IGNORED && h.isUp())
                        addOrRenewPool(h);
                } else if (dist != pool.hostDistance) {
                    if (dist == HostDistance.IGNORED) {
                        removePool(h);
                    } else {
                        pool.hostDistance = dist;
                    }
                }
            }
        }

        @Override
        public void onUp(Host host) {
            addOrRenewPool(host).addListener(new Runnable() {
                public void run() {
                    updateCreatedPools();
                }
            }, MoreExecutors.sameThreadExecutor());
        }

        @Override
        public void onDown(Host host) {
            // Note that with well behaved balancing policy (that ignore dead nodes), the removePool call is not necessary
            // since updateCreatedPools should take care of it. But better protect against non well behaving policies.
            removePool(host).addListener(new Runnable() {
                public void run() {
                    updateCreatedPools();
                }
            }, MoreExecutors.sameThreadExecutor());
        }

        @Override
        public void onAdd(Host host) {
            onUp(host);
        }

        @Override
        public void onRemove(Host host) {
            onDown(host);
        }

        public void setKeyspace(String keyspace) {
            long timeout = configuration().getSocketOptions().getConnectTimeoutMillis();
            try {
                Future<?> future = executeQuery(new QueryMessage("use " + keyspace, ConsistencyLevel.DEFAULT_CASSANDRA_CL), Query.DEFAULT);
                // Note: using the connection timeout is perfectly correct, we should probably change that someday
                Uninterruptibles.getUninterruptibly(future, timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new DriverInternalError(String.format("No responses after %d milliseconds while setting current keyspace. This should not happen, unless you have setup a very low connection timeout.", timeout));
            } catch (ExecutionException e) {
                ResultSetFuture.extractCauseFromExecutionException(e);
            }
        }

        /**
         * Execute the provided request.
         *
         * This method will find a suitable node to connect to using the
         * {@link LoadBalancingPolicy} and handle host failover.
         */
        public void execute(RequestHandler.Callback callback, Query query) {
            new RequestHandler(this, callback, query).sendRequest();
        }

        public void prepare(String query, InetAddress toExclude) throws InterruptedException {
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

        public ResultSetFuture executeQuery(Message.Request msg, Query query) {
            if (query.isTracing())
                msg.setTracingRequested();

            ResultSetFuture future = new ResultSetFuture(this, msg);
            execute(future.callback, query);
            return future;
        }
    }
}
