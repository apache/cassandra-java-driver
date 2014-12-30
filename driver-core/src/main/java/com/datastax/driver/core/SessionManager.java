/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;

/**
 * Driver implementation of the Session interface.
 */
class SessionManager extends AbstractSession {

    private static final Logger logger = LoggerFactory.getLogger(Session.class);

    final Cluster cluster;
    final ConcurrentMap<Host, HostConnectionPool> pools;
    final HostConnectionPool.PoolState poolsState;
    final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

    private final Striped<Lock> poolCreationLocks = Striped.lazyWeakLock(5);

    private volatile boolean isInit;
    private volatile boolean isClosing;

    // Package protected, only Cluster should construct that.
    SessionManager(Cluster cluster) {
        this.cluster = cluster;
        this.pools = new ConcurrentHashMap<Host, HostConnectionPool>();
        this.poolsState = new HostConnectionPool.PoolState();
    }

    public synchronized Session init() {
        if (isInit)
            return this;

        // If we haven't initialized the cluster, do it now
        cluster.init();

        // Create pools to initial nodes (and wait for them to be created)
        Collection<Host> hosts = cluster.getMetadata().allHosts();
        if (cluster.manager.sessions.size() == 1) {
            // We only do it in parallel if this is the first session (meaning that the cluster just initialized).
            createPoolsInParallel(hosts);
        } else {
            // Otherwise, we don't want to fill executor() because this is also where up/down notifications are processed,
            // it's important that existing sessions get them in a timely manner. So we create the pools one by one:
            createPoolsSequentially(hosts);
        }

        isInit = true;
        updateCreatedPools(executor());
        return this;
    }

    private void createPoolsInParallel(Collection<Host> hosts) {
        List<ListenableFuture<Boolean>> futures = new ArrayList<ListenableFuture<Boolean>>(hosts.size());
        for (Host host : hosts)
            if (host.state != Host.State.DOWN)
                futures.add(maybeAddPool(host, executor()));
        ListenableFuture<List<Boolean>> f = Futures.allAsList(futures);
        try {
            f.get();
        } catch (ExecutionException e) {
            // This is not supposed to happen
            throw new DriverInternalError(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void createPoolsSequentially(Collection<Host> hosts) {
        for (Host host : cluster.getMetadata().allHosts()) {
            try {
                if (host.state != Host.State.DOWN)
                    maybeAddPool(host, executor()).get();
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

    public ResultSetFuture executeAsync(Statement statement) {
        return executeQuery(makeRequestMessage(statement, null), statement);
    }

    public ListenableFuture<PreparedStatement> prepareAsync(String query) {
        Connection.Future future = new Connection.Future(new Requests.Prepare(query));
        execute(future, Statement.DEFAULT);
        return toPreparedStatement(query, future);
    }

    public CloseFuture closeAsync() {
        CloseFuture future = closeFuture.get();
        if (future != null)
            return future;

        isClosing = true;
        cluster.manager.removeSession(this);

        List<CloseFuture> futures = new ArrayList<CloseFuture>(pools.size());
        for (HostConnectionPool pool : pools.values())
            futures.add(pool.closeAsync());

        future = new CloseFuture.Forwarding(futures);

        return closeFuture.compareAndSet(null, future)
            ? future
            : closeFuture.get(); // We raced, it's ok, return the future that was actually set
    }

    public boolean isClosed() {
        return closeFuture.get() != null;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Session.State getState() {
        return new State(this);
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
                                PreparedStatement stmt = DefaultPreparedStatement.fromMessage(pmsg, cluster.getMetadata(), query, poolsState.keyspace);
                                stmt = cluster.manager.addPrepared(stmt);
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
        return cluster.manager.blockingExecutor;
    }

    // Returns whether there was problem creating the pool
    ListenableFuture<Boolean> forceRenewPool(final Host host, ListeningExecutorService executor) {
        final HostDistance distance = cluster.manager.loadBalancingPolicy().distance(host);
        if (distance == HostDistance.IGNORED)
            return Futures.immediateFuture(true);

        // Creating a pool is somewhat long since it has to create the connection, so do it asynchronously.
        return executor.submit(new Callable<Boolean>() {
            public Boolean call() {
                while (true) {
                    try {
                        if (isClosing)
                            return true;

                        HostConnectionPool newPool = new HostConnectionPool(host, distance, SessionManager.this);
                        HostConnectionPool previous = pools.put(host, newPool);
                        if (previous == null) {
                            logger.debug("Added connection pool for {}", host);
                        } else {
                            logger.debug("Renewed connection pool for {}", host);
                            previous.closeAsync();
                        }

                        // If we raced with a session shutdown, ensure that the pool will be closed.
                        if (isClosing)
                            newPool.closeAsync();

                        return true;
                    } catch (Exception e) {
                        logger.error("Error creating pool to " + host, e);
                        return false;
                    }
                }
            }
        });
    }

    // Replace pool for a given only if it's the given previous value (which can be null)
    // Note that the goal of this function is to make sure that 2 concurrent thread calling
    // maybeAddPool don't end up creating 2 HostConnectionPool. We can't rely on the pools
    // ConcurrentMap only for that since it's the duplicate HostConnectionPool creation we
    // want to avoid
    private boolean replacePool(Host host, HostDistance distance, HostConnectionPool condition) throws ConnectionException, UnsupportedProtocolVersionException, ClusterNameMismatchException {
        if (isClosing)
            return true;

        Lock l = poolCreationLocks.get(host);
        l.lock();
        try {
            HostConnectionPool previous = pools.get(host);
            if (previous != condition)
                return false;

            HostConnectionPool newPool = new HostConnectionPool(host, distance, this);
            previous = pools.put(host, newPool);
            if (previous != null && !previous.isClosed()) {
                logger.warn("Replacing a pool that wasn't closed. Closing it now, but this was not expected.");
                previous.closeAsync();
            }

            // If we raced with a session shutdown, ensure that the pool will be closed.
            if (isClosing)
                newPool.closeAsync();

            return true;
        } finally {
            l.unlock();
        }
    }

    // Returns whether there was problem creating the pool
    ListenableFuture<Boolean> maybeAddPool(final Host host, ListeningExecutorService executor) {
        final HostDistance distance = cluster.manager.loadBalancingPolicy().distance(host);
        if (distance == HostDistance.IGNORED)
            return Futures.immediateFuture(true);

        HostConnectionPool previous = pools.get(host);
        if (previous != null && !previous.isClosed())
            return Futures.immediateFuture(true);

        // Creating a pool is somewhat long since it has to create the connection, so do it asynchronously.
        return executor.submit(new Callable<Boolean>() {
            public Boolean call() {
                try {
                    while (true) {
                        HostConnectionPool previous = pools.get(host);
                        if (previous != null && !previous.isClosed())
                            return true;

                        if (replacePool(host, distance, previous)) {
                            logger.debug("Added connection pool for {}", host);
                            return true;
                        }
                    }
                } catch (UnsupportedProtocolVersionException e) {
                    cluster.manager.logUnsupportedVersionProtocol(host);
                    cluster.manager.triggerOnDown(host, false);
                    return false;
                } catch (ClusterNameMismatchException e) {
                    cluster.manager.logClusterNameMismatch(host, e.expectedClusterName, e.actualClusterName);
                    cluster.manager.triggerOnDown(host, false);
                    return false;
                } catch (Exception e) {
                    logger.error("Error creating pool to " + host, e);
                    return false;
                }
            }
        });
    }

    CloseFuture removePool(Host host) {
        final HostConnectionPool pool = pools.remove(host);
        return pool == null
             ? CloseFuture.immediateFuture()
             : pool.closeAsync();
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
    void updateCreatedPools(ListeningExecutorService executor) {
        // This method does nothing during initialization. Some hosts may be non-responsive but not yet marked DOWN; if
        // we execute the code below we would try to create their pool over and over again.
        // It's called explicitly at the end of init(), once isInit has been set to true.
        if (!isInit)
            return;

        try {
            // We do 2 iterations, so that we add missing pools first, and them remove all unecessary pool second.
            // That way, we'll avoid situation where we'll temporarily lose connectivity
            List<Host> toRemove = new ArrayList<Host>();
            List<ListenableFuture<?>> poolCreationFutures = new ArrayList<ListenableFuture<?>>();

            for (Host h : cluster.getMetadata().allHosts()) {
                HostDistance dist = loadBalancingPolicy().distance(h);
                HostConnectionPool pool = pools.get(h);

                if (pool == null) {
                    if (dist != HostDistance.IGNORED && h.state == Host.State.UP)
                        poolCreationFutures.add(maybeAddPool(h, executor));
                } else if (dist != pool.hostDistance) {
                    if (dist == HostDistance.IGNORED) {
                        toRemove.add(h);
                    } else {
                        pool.hostDistance = dist;
                        pool.ensureCoreConnections();
                    }
                }
            }

            // Wait pool creation before removing, so we don't lose connectivity
            Futures.allAsList(poolCreationFutures).get();

            List<ListenableFuture<?>> poolRemovalFutures = new ArrayList<ListenableFuture<?>>(toRemove.size());
            for (Host h : toRemove)
                poolRemovalFutures.add(removePool(h));

            Futures.allAsList(poolRemovalFutures).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Unexpected error while refreshing connection pools", e.getCause());
        }
    }

    void updateCreatedPools(Host h, ListeningExecutorService executor) {
        HostDistance dist = loadBalancingPolicy().distance(h);
        HostConnectionPool pool = pools.get(h);

        try {
            if (pool == null) {
                if (dist != HostDistance.IGNORED && h.state == Host.State.UP)
                    maybeAddPool(h, executor).get();
            } else if (dist != pool.hostDistance) {
                if (dist == HostDistance.IGNORED) {
                    removePool(h).get();
                } else {
                    pool.hostDistance = dist;
                    pool.ensureCoreConnections();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Unexpected error while refreshing connection pools", e.getCause());
        }
    }

    void onDown(Host host) throws InterruptedException, ExecutionException {
        // Note that with well behaved balancing policy (that ignore dead nodes), the removePool call is not necessary
        // since updateCreatedPools should take care of it. But better protect against non well behaving policies.
        removePool(host).force().get();
        updateCreatedPools(MoreExecutors.sameThreadExecutor());
    }

    void onSuspected(Host host) {
    }

    void onRemove(Host host) throws InterruptedException, ExecutionException {
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
        // init() locks, so avoid if we know we don't need it.
        if (!isInit)
            init();
        new RequestHandler(this, callback, statement).sendRequest();
    }

    private void prepare(String query, InetSocketAddress toExclude) throws InterruptedException {
        for (Map.Entry<Host, HostConnectionPool> entry : pools.entrySet()) {
            if (entry.getKey().getSocketAddress().equals(toExclude))
                continue;

            // Let's not wait too long if we can't get a connection. Things
            // will fix themselves once the user tries a query anyway.
            PooledConnection c = null;
            boolean timedOut = false;
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
                // If the query timed out, that already released the connection
                timedOut = e.getCause() instanceof OperationTimedOutException;
            } finally {
                if (c != null && !timedOut)
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

    void cleanupIdleConnections(long now) {
        for (HostConnectionPool pool : pools.values()) {
            pool.cleanupIdleConnections(now);
        }
    }

    private static class State implements Session.State {

        private final SessionManager session;
        private final List<Host> connectedHosts;
        private final int[] openConnections;
        private final int[] inFlightQueries;

        private State(SessionManager session) {
            this.session = session;
            this.connectedHosts = ImmutableList.copyOf(session.pools.keySet());

            this.openConnections = new int[connectedHosts.size()];
            this.inFlightQueries = new int[connectedHosts.size()];

            int i = 0;
            for (Host h : connectedHosts) {
                HostConnectionPool p = session.pools.get(h);
                // It's possible we race and the host has been removed since the beginning of this
                // functions. In that case, the fact it's part of getConnectedHosts() but has no opened
                // connections will be slightly weird, but it's unlikely enough that we don't bother avoiding.
                if (p == null) {
                    openConnections[i] = 0;
                    inFlightQueries[i] = 0;
                    continue;
                }

                openConnections[i] = p.connections.size();
                for (Connection c : p.connections) {
                    inFlightQueries[i] += c.inFlight.get();
                }
                i++;
            }
        }

        private int getIdx(Host h) {
            // We guarantee that we only ever create one Host object per-address, which means that '=='
            // comparison is a proper way to test Host equality. Given that, the number of hosts
            // per-session will always be small enough (even 1000 is kind of small and even with a 1000+
            // node cluster, you probably don't want a Session to connect to all of them) that iterating
            // over connectedHosts will never be much more inefficient than keeping a
            // Map<Host, SomeStructureForHostInfo>. And it's less garbage/memory consumption so...
            for (int i = 0; i < connectedHosts.size(); i++)
                if (h == connectedHosts.get(i))
                    return i;
            return -1;
        }

        public Session getSession() {
            return session;
        }

        public Collection<Host> getConnectedHosts() {
            return connectedHosts;
        }

        public int getOpenConnections(Host host) {
            int i = getIdx(host);
            return i < 0 ? 0 : openConnections[i];
        }

        public int getInFlightQueries(Host host) {
            int i = getIdx(host);
            return i < 0 ? 0 : inFlightQueries[i];
        }
    }
}
