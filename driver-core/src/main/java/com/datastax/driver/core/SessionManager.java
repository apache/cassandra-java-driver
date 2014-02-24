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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Function;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;

/**
 * Driver implementation of the Session interface.
 */
class SessionManager implements Session {

    private static final Logger logger = LoggerFactory.getLogger(Session.class);

    final Cluster cluster;
    final ConcurrentMap<Host, HostConnectionPool> pools;
    final HostConnectionPool.PoolState poolsState;
    final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

    // Package protected, only Cluster should construct that.
    SessionManager(Cluster cluster, Collection<Host> hosts) {
        this.cluster = cluster;

        this.pools = new ConcurrentHashMap<Host, HostConnectionPool>(hosts.size());
        this.poolsState = new HostConnectionPool.PoolState();

        // Create pool to initial nodes (and wait for them to be created)
        for (Host host : hosts) {
            try {
                addOrRenewPool(host, false).get();
            } catch (ExecutionException e) {
                // This is not supposed to happen
                throw new DriverInternalError(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public String getLoggedKeyspace() {
        return poolsState.keyspace;
    }

    public ResultSet execute(String query) {
        return execute(new SimpleStatement(query));
    }

    public ResultSet execute(String query, Object... values) {
        return execute(new SimpleStatement(query, values));
    }

    public ResultSet execute(Statement statement) {
        return executeAsync(statement).getUninterruptibly();
    }

    public ResultSetFuture executeAsync(String query) {
        return executeAsync(new SimpleStatement(query));
    }

    public ResultSetFuture executeAsync(String query, Object... values) {
        return executeAsync(new SimpleStatement(query, values));
    }

    public ResultSetFuture executeAsync(Statement statement) {
        return executeQuery(makeRequestMessage(statement, null), statement);
    }

    public PreparedStatement prepare(String query) {
        try {
            return Uninterruptibles.getUninterruptibly(prepareAsync(query));
        } catch (ExecutionException e) {
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
        }
    }

    public PreparedStatement prepare(RegularStatement statement) {
        try {
            return Uninterruptibles.getUninterruptibly(prepareAsync(statement));
        } catch (ExecutionException e) {
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
        }
    }

    public ListenableFuture<PreparedStatement> prepareAsync(String query) {
        Connection.Future future = new Connection.Future(new Requests.Prepare(query));
        execute(future, Statement.DEFAULT);
        return toPreparedStatement(query, future);
    }

    public ListenableFuture<PreparedStatement> prepareAsync(final RegularStatement statement) {
        if (statement.getValues() != null)
            throw new IllegalArgumentException("A statement to prepare should not have values");

        ListenableFuture<PreparedStatement> prepared = prepareAsync(statement.toString());
        return Futures.transform(prepared, new Function<PreparedStatement, PreparedStatement>() {
            @Override
            public PreparedStatement apply(PreparedStatement prepared) {
                ByteBuffer routingKey = statement.getRoutingKey();
                if (routingKey != null)
                    prepared.setRoutingKey(routingKey);
                prepared.setConsistencyLevel(statement.getConsistencyLevel());
                if (statement.isTracing())
                    prepared.enableTracing();
                prepared.setRetryPolicy(statement.getRetryPolicy());

                return prepared;
            }
        });
    }

    public CloseFuture closeAsync() {
        CloseFuture future = closeFuture.get();
        if (future != null)
            return future;

        List<CloseFuture> futures = new ArrayList<CloseFuture>(pools.size());
        for (HostConnectionPool pool : pools.values())
            futures.add(pool.closeAsync());

        future = new CloseFuture.Forwarding(futures);

        return closeFuture.compareAndSet(null, future)
            ? future
            : closeFuture.get(); // We raced, it's ok, return the future that was actually set
    }

    public void close() {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    private ListenableFuture<PreparedStatement> toPreparedStatement(final String query, final Connection.Future future) {
        return Futures.transform(future, new Function<Message.Response, PreparedStatement>() {
            public PreparedStatement apply(Message.Response response) {
                switch (response.type) {
                    case RESULT:
                        Responses.Result rm = (Responses.Result)response;
                        switch (rm.kind) {
                            case PREPARED:
                                Responses.Result.Prepared pmsg = (Responses.Result.Prepared)rm;
                                DefaultPreparedStatement stmt = DefaultPreparedStatement.fromMessage(pmsg, cluster.getMetadata(), query, poolsState.keyspace);
                                cluster.manager.addPrepared(stmt);
                                try {
                                    // All Sessions are connected to the same nodes so it's enough to prepare only the nodes of this session.
                                    // If that changes, we'll have to make sure this propagate to other sessions too.
                                    prepare(stmt.getQueryString(), future.getAddress());
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
                        throw ((Responses.Error)response).asException(future.getAddress());
                    default:
                        throw new DriverInternalError(String.format("%s response received when prepared statement was expected", response.type));
                }
            }
        }, executor()); // Since the transformation involves querying other nodes, we should not do that in an I/O thread
    }

    Connection.Factory connectionFactory() {
        return cluster.manager.connectionFactory;
    }

    Configuration configuration() {
        return cluster.manager.configuration;
    }

    LoadBalancingPolicy loadBalancingPolicy() {
        return cluster.manager.loadBalancingPolicy();
    }

    ReconnectionPolicy reconnectionPolicy() {
        return cluster.manager.reconnectionPolicy();
    }

    ListeningExecutorService executor() {
        return cluster.manager.executor;
    }

    ListeningExecutorService blockingExecutor() {
        return cluster.manager.blockingTasksExecutor;
    }

    boolean isClosed() {
        return closeFuture.get() != null;
    }

    ListenableFuture<Boolean> addOrRenewPool(final Host host, final boolean isHostAddition) {
        final HostDistance distance = cluster.manager.loadBalancingPolicy().distance(host);
        if (distance == HostDistance.IGNORED)
            return Futures.immediateFuture(true);

        // Creating a pool is somewhat long since it has to create the connection, so do it asynchronously.
        return executor().submit(new Callable<Boolean>() {
            public Boolean call() {
                logger.debug("Adding {} to list of queried hosts", host);
                try {
                    HostConnectionPool previous = pools.put(host, new HostConnectionPool(host, distance, SessionManager.this));
                    if (previous != null)
                        previous.closeAsync(); // The previous was probably already shutdown but that's ok
                    return true;
                } catch (AuthenticationException e) {
                    logger.error("Error creating pool to {} ({})", host, e.getMessage());
                    cluster.manager.signalConnectionFailure(host, new ConnectionException(e.getHost(), e.getMessage()), isHostAddition);
                    return false;
                } catch (UnsupportedProtocolVersionException e) {
                    logger.error("Error creating pool to {} ({})", host, e.getMessage());
                    cluster.manager.signalConnectionFailure(host, new ConnectionException(e.address, e.getMessage()), isHostAddition);
                    return false;
                } catch (ConnectionException e) {
                    logger.debug("Error creating pool to {} ({})", host, e.getMessage());
                    cluster.manager.signalConnectionFailure(host, e, isHostAddition);
                    return false;
                }
            }
        });
    }

    ListenableFuture<?> removePool(Host host) {
        final HostConnectionPool pool = pools.remove(host);
        if (pool == null)
            return Futures.immediateFuture(null);

        // closeAsync can take some time and we don't care about holding the thread on that.
        return executor().submit(new Runnable() {
            public void run() {
                pool.closeAsync();
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
    void updateCreatedPools() {
        for (Host h : cluster.getMetadata().allHosts()) {
            HostDistance dist = loadBalancingPolicy().distance(h);
            HostConnectionPool pool = pools.get(h);

            if (pool == null) {
                if (dist != HostDistance.IGNORED && h.isUp())
                    addOrRenewPool(h, false);
            } else if (dist != pool.hostDistance) {
                if (dist == HostDistance.IGNORED) {
                    removePool(h);
                } else {
                    pool.hostDistance = dist;
                }
            }
        }
    }

    void onDown(Host host) {
        // Note that with well behaved balancing policy (that ignore dead nodes), the removePool call is not necessary
        // since updateCreatedPools should take care of it. But better protect against non well behaving policies.
        removePool(host).addListener(new Runnable() {
            public void run() {
                updateCreatedPools();
            }
        }, MoreExecutors.sameThreadExecutor());
    }

    void onRemove(Host host) {
        onDown(host);
    }

    void setKeyspace(String keyspace) {
        long timeout = configuration().getSocketOptions().getConnectTimeoutMillis();
        try {
            Future<?> future = executeQuery(new Requests.Query("use " + keyspace), Statement.DEFAULT);
            // Note: using the connection timeout isn't perfectly correct, we should probably change that someday
            Uninterruptibles.getUninterruptibly(future, timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new DriverInternalError(String.format("No responses after %d milliseconds while setting current keyspace. This should not happen, unless you have setup a very low connection timeout.", timeout));
        } catch (ExecutionException e) {
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
        }
    }

    Message.Request makeRequestMessage(Statement statement, ByteBuffer pagingState) {

        ConsistencyLevel consistency = statement.getConsistencyLevel();
        if (consistency == null)
            consistency = configuration().getQueryOptions().getConsistencyLevel();

        ConsistencyLevel serialConsistency = statement.getSerialConsistencyLevel();
        if (serialConsistency == null)
            serialConsistency = configuration().getQueryOptions().getSerialConsistencyLevel();

        return makeRequestMessage(statement, consistency, serialConsistency, pagingState);
    }

    Message.Request makeRequestMessage(Statement statement, ConsistencyLevel cl, ConsistencyLevel scl, ByteBuffer pagingState) {
        int protoVersion = cluster.manager.protocolVersion();
        int fetchSize = statement.getFetchSize();

        if (protoVersion == 1) {
            assert pagingState == null;
            // We don't let the user change the fetchSize globally if the proto v1 is used, so we just need to
            // check for the case of a per-statement override
            if (fetchSize <= 0)
                fetchSize = -1;
            else if (fetchSize != Integer.MAX_VALUE)
                throw new UnsupportedFeatureException("Paging is not supported");
        } else if (fetchSize <= 0) {
            fetchSize = configuration().getQueryOptions().getFetchSize();
        }

        if (fetchSize == Integer.MAX_VALUE)
            fetchSize = -1;

        if (statement instanceof RegularStatement) {
            RegularStatement rs = (RegularStatement)statement;

            // It saddens me that we special case for the query builder here, but for now this is simpler.
            // We could provide a general API in RegularStatement instead at some point but it's unclear what's
            // the cleanest way to do that is right now (and it's probably not really that useful anyway).
            if (protoVersion == 1 && rs instanceof com.datastax.driver.core.querybuilder.BuiltStatement)
                ((com.datastax.driver.core.querybuilder.BuiltStatement)rs).setForceNoValues(true);

            ByteBuffer[] rawValues = rs.getValues();

            if (protoVersion == 1 && rawValues != null)
                throw new UnsupportedFeatureException("Binary values are not supported");

            List<ByteBuffer> values = rawValues == null ? Collections.<ByteBuffer>emptyList() : Arrays.asList(rawValues);
            String qString = rs.getQueryString();
            Requests.QueryProtocolOptions options = new Requests.QueryProtocolOptions(cl, values, false, fetchSize, pagingState, scl);
            return new Requests.Query(qString, options);
        } else if (statement instanceof BoundStatement) {
            BoundStatement bs = (BoundStatement)statement;
            boolean skipMetadata = protoVersion != 1 && bs.statement.getPreparedId().resultSetMetadata != null;
            Requests.QueryProtocolOptions options = new Requests.QueryProtocolOptions(cl, Arrays.asList(bs.values), skipMetadata, fetchSize, pagingState, scl);
            return new Requests.Execute(bs.statement.getPreparedId().id, options);
        } else {
            assert statement instanceof BatchStatement : statement;
            assert pagingState == null;

            if (protoVersion == 1)
                throw new UnsupportedFeatureException("Protocol level batching is not supported");

            BatchStatement bs = (BatchStatement)statement;
            BatchStatement.IdAndValues idAndVals = bs.getIdAndValues();
            return new Requests.Batch(bs.batchType, idAndVals.ids, idAndVals.values, cl);
        }
    }

    /**
     * Execute the provided request.
     *
     * This method will find a suitable node to connect to using the
     * {@link LoadBalancingPolicy} and handle host failover.
     */
    void execute(RequestHandler.Callback callback, Statement statement) {
        new RequestHandler(this, callback, statement).sendRequest();
    }

    private void prepare(String query, InetAddress toExclude) throws InterruptedException {
        for (Map.Entry<Host, HostConnectionPool> entry : pools.entrySet()) {
            if (entry.getKey().getAddress().equals(toExclude))
                continue;

            // Let's not wait too long if we can't get a connection. Things
            // will fix themselves once the user tries a query anyway.
            PooledConnection c = null;
            try {
                c = entry.getValue().borrowConnection(200, TimeUnit.MILLISECONDS);
                c.write(new Requests.Prepare(query)).get();
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
                    c.release();
            }
        }
    }

    ResultSetFuture executeQuery(Message.Request msg, Statement statement) {
        if (statement.isTracing())
            msg.setTracingRequested();

        DefaultResultSetFuture future = new DefaultResultSetFuture(this, msg);
        execute(future, statement);
        return future;
    }
}
