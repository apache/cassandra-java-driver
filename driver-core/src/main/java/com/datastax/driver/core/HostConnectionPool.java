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

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.BusyPoolException;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.UnsupportedProtocolVersionException;
import com.datastax.driver.core.utils.MoreFutures;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import io.netty.util.concurrent.EventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.driver.core.Connection.State.*;

class HostConnectionPool implements Connection.Owner {

    private static final Logger logger = LoggerFactory.getLogger(HostConnectionPool.class);

    private static final int MAX_SIMULTANEOUS_CREATION = 1;

    final Host host;
    volatile HostDistance hostDistance;
    protected final SessionManager manager;

    final List<Connection> connections;
    private final AtomicInteger open;
    /**
     * The total number of in-flight requests on all connections of this pool.
     */
    final AtomicInteger totalInFlight = new AtomicInteger();
    /**
     * The maximum value of {@link #totalInFlight} since the last call to {@link #cleanupIdleConnections(long)}
     */
    private final AtomicInteger maxTotalInFlight = new AtomicInteger();
    @VisibleForTesting
    final Set<Connection> trash = new CopyOnWriteArraySet<Connection>();

    private final Queue<PendingBorrow> pendingBorrows = new ConcurrentLinkedQueue<PendingBorrow>();
    private final AtomicInteger pendingBorrowCount = new AtomicInteger();

    private final Runnable newConnectionTask;

    private final AtomicInteger scheduledForCreation = new AtomicInteger();

    private final EventExecutor timeoutsExecutor;

    private final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

    private enum Phase {INITIALIZING, READY, INIT_FAILED, CLOSING}

    protected final AtomicReference<Phase> phase = new AtomicReference<Phase>(Phase.INITIALIZING);

    // When a request times out, we may never release its stream ID. So over time, a given connection
    // may get less an less available streams. When the number of available ones go below the
    // following threshold, we just replace the connection by a new one.
    private final int minAllowedStreams;

    HostConnectionPool(Host host, HostDistance hostDistance, SessionManager manager) {
        assert hostDistance != HostDistance.IGNORED;
        this.host = host;
        this.hostDistance = hostDistance;
        this.manager = manager;

        this.newConnectionTask = new Runnable() {
            @Override
            public void run() {
                addConnectionIfUnderMaximum();
                scheduledForCreation.decrementAndGet();
            }
        };

        this.connections = new CopyOnWriteArrayList<Connection>();
        this.open = new AtomicInteger();

        this.minAllowedStreams = options().getMaxRequestsPerConnection(hostDistance) * 3 / 4;

        this.timeoutsExecutor = manager.getCluster().manager.connectionFactory.eventLoopGroup.next();
    }

    /**
     * @param reusedConnection an existing connection (from a reconnection attempt) that we want to
     *                         reuse as part of this pool. Might be null or already used by another
     *                         pool.
     */
    ListenableFuture<Void> initAsync(Connection reusedConnection) {
        Executor initExecutor = manager.cluster.manager.configuration.getPoolingOptions().getInitializationExecutor();

        // Create initial core connections
        final int coreSize = options().getCoreConnectionsPerHost(hostDistance);
        final List<Connection> connections = Lists.newArrayListWithCapacity(coreSize);
        final List<ListenableFuture<Void>> connectionFutures = Lists.newArrayListWithCapacity(coreSize);

        int toCreate = coreSize;

        if (reusedConnection != null && reusedConnection.setOwner(this)) {
            toCreate -= 1;
            connections.add(reusedConnection);
            connectionFutures.add(MoreFutures.VOID_SUCCESS);
        }

        List<Connection> newConnections = manager.connectionFactory().newConnections(this, toCreate);
        connections.addAll(newConnections);
        for (Connection connection : newConnections) {
            ListenableFuture<Void> connectionFuture = connection.initAsync();
            connectionFutures.add(handleErrors(connectionFuture, initExecutor));
        }

        ListenableFuture<List<Void>> allConnectionsFuture = Futures.allAsList(connectionFutures);

        final SettableFuture<Void> initFuture = SettableFuture.create();
        Futures.addCallback(allConnectionsFuture, new FutureCallback<List<Void>>() {
            @Override
            public void onSuccess(List<Void> l) {
                // Some of the connections might have failed, keep only the successful ones
                ListIterator<Connection> it = connections.listIterator();
                while (it.hasNext()) {
                    if (it.next().isClosed())
                        it.remove();
                }

                HostConnectionPool.this.connections.addAll(connections);
                open.set(connections.size());

                if (isClosed()) {
                    initFuture.setException(new ConnectionException(host.getSocketAddress(), "Pool was closed during initialization"));
                    // we're not sure if closeAsync() saw the connections, so ensure they get closed
                    forceClose(connections);
                } else {
                    logger.debug("Created connection pool to host {} ({} connections needed, {} successfully opened)",
                            host, coreSize, connections.size());
                    phase.compareAndSet(Phase.INITIALIZING, Phase.READY);
                    initFuture.set(null);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                phase.compareAndSet(Phase.INITIALIZING, Phase.INIT_FAILED);
                forceClose(connections);
                initFuture.setException(t);
            }
        }, initExecutor);
        return initFuture;
    }

    private ListenableFuture<Void> handleErrors(ListenableFuture<Void> connectionInitFuture, Executor executor) {
        return GuavaCompatibility.INSTANCE.withFallback(connectionInitFuture, new AsyncFunction<Throwable, Void>() {
            @Override
            public ListenableFuture<Void> apply(Throwable t) throws Exception {
                // Propagate these exceptions because they mean no connection will ever succeed. They will be handled
                // accordingly in SessionManager#maybeAddPool.
                Throwables.propagateIfInstanceOf(t, ClusterNameMismatchException.class);
                Throwables.propagateIfInstanceOf(t, UnsupportedProtocolVersionException.class);

                // We don't want to swallow Errors either as they probably indicate a more serious issue (OOME...)
                Throwables.propagateIfInstanceOf(t, Error.class);

                // Otherwise, return success. The pool will simply ignore this connection when it sees that it's been closed.
                return MoreFutures.VOID_SUCCESS;
            }
        }, executor);
    }

    // Clean up if we got a fatal error at construction time but still created part of the core connections
    private void forceClose(List<Connection> connections) {
        for (Connection connection : connections) {
            connection.closeAsync().force();
        }
    }

    private PoolingOptions options() {
        return manager.configuration().getPoolingOptions();
    }

    ListenableFuture<Connection> borrowConnection(long timeout, TimeUnit unit, int maxQueueSize) {
        Phase phase = this.phase.get();
        if (phase != Phase.READY)
            return Futures.immediateFailedFuture(new ConnectionException(host.getSocketAddress(), "Pool is " + phase));

        if (connections.isEmpty()) {
            if (host.convictionPolicy.canReconnectNow()) {
                int coreSize = options().getCoreConnectionsPerHost(hostDistance);
                if (coreSize == 0) {
                    maybeSpawnNewConnection();
                } else if (scheduledForCreation.compareAndSet(0, coreSize)) {
                    for (int i = 0; i < coreSize; i++) {
                        // We don't respect MAX_SIMULTANEOUS_CREATION here because it's  only to
                        // protect against creating connection in excess of core too quickly
                        manager.blockingExecutor().submit(newConnectionTask);
                    }
                }
                return enqueue(timeout, unit, maxQueueSize);
            }
        }

        int minInFlight = Integer.MAX_VALUE;
        Connection leastBusy = null;
        for (Connection connection : connections) {
            int inFlight = connection.inFlight.get();
            if (inFlight < minInFlight) {
                minInFlight = inFlight;
                leastBusy = connection;
            }
        }

        if (leastBusy == null) {
            // We could have raced with a shutdown since the last check
            if (isClosed())
                return Futures.immediateFailedFuture(new ConnectionException(host.getSocketAddress(), "Pool is shutdown"));
            // This might maybe happen if the number of core connections per host is 0 and a connection was trashed between
            // the previous check to connections and now. But in that case, the line above will have trigger the creation of
            // a new connection, so just wait that connection and move on
            return enqueue(timeout, unit, maxQueueSize);
        } else {
            while (true) {
                int inFlight = leastBusy.inFlight.get();

                if (inFlight >= Math.min(leastBusy.maxAvailableStreams(), options().getMaxRequestsPerConnection(hostDistance))) {
                    return enqueue(timeout, unit, maxQueueSize);
                }

                if (leastBusy.inFlight.compareAndSet(inFlight, inFlight + 1))
                    break;
            }
        }

        int totalInFlightCount = totalInFlight.incrementAndGet();
        // update max atomically:
        while (true) {
            int oldMax = maxTotalInFlight.get();
            if (totalInFlightCount <= oldMax || maxTotalInFlight.compareAndSet(oldMax, totalInFlightCount))
                break;
        }

        int connectionCount = open.get() + scheduledForCreation.get();
        if (connectionCount < options().getCoreConnectionsPerHost(hostDistance)) {
            maybeSpawnNewConnection();
        } else if (connectionCount < options().getMaxConnectionsPerHost(hostDistance)) {
            // Add a connection if we fill the first n-1 connections and almost fill the last one
            int currentCapacity = (connectionCount - 1) * options().getMaxRequestsPerConnection(hostDistance)
                    + options().getNewConnectionThreshold(hostDistance);
            if (totalInFlightCount > currentCapacity)
                maybeSpawnNewConnection();
        }

        return leastBusy.setKeyspaceAsync(manager.poolsState.keyspace);
    }

    private ListenableFuture<Connection> enqueue(long timeout, TimeUnit unit, int maxQueueSize) {
        if (timeout == 0 || maxQueueSize == 0) {
            return Futures.immediateFailedFuture(new BusyPoolException(host.getSocketAddress(), 0));
        }

        while (true) {
            int count = pendingBorrowCount.get();
            if (count >= maxQueueSize) {
                return Futures.immediateFailedFuture(new BusyPoolException(host.getSocketAddress(), maxQueueSize));
            }
            if (pendingBorrowCount.compareAndSet(count, count + 1)) {
                break;
            }
        }

        PendingBorrow pendingBorrow = new PendingBorrow(timeout, unit, timeoutsExecutor);
        pendingBorrows.add(pendingBorrow);

        // If we raced with shutdown, make sure the future will be completed. This has no effect if it was properly
        // handled in closeAsync.
        if (phase.get() == Phase.CLOSING) {
            pendingBorrow.setException(new ConnectionException(host.getSocketAddress(), "Pool is shutdown"));
        }

        return pendingBorrow.future;
    }

    void returnConnection(Connection connection) {
        connection.inFlight.decrementAndGet();
        totalInFlight.decrementAndGet();

        if (isClosed()) {
            close(connection);
            return;
        }

        if (connection.isDefunct()) {
            // As part of making it defunct, we have already replaced it or
            // closed the pool.
            return;
        }

        if (connection.state.get() != TRASHED) {
            if (connection.maxAvailableStreams() < minAllowedStreams) {
                replaceConnection(connection);
            } else {
                dequeue(connection);
            }
        }
    }

    // When a connection gets returned to the pool, check if there are pending borrows that can be completed with it.
    private void dequeue(final Connection connection) {
        while (!pendingBorrows.isEmpty()) {

            // We can only reuse the connection if it's under its maximum number of inFlight requests.
            // Do this atomically, as we could be competing with other borrowConnection or dequeue calls.
            while (true) {
                int inFlight = connection.inFlight.get();
                if (inFlight >= Math.min(connection.maxAvailableStreams(), options().getMaxRequestsPerConnection(hostDistance))) {
                    // Connection is full again, stop dequeuing
                    return;
                }
                if (connection.inFlight.compareAndSet(inFlight, inFlight + 1)) {
                    // We acquired the right to reuse the connection for one request, proceed
                    break;
                }
            }

            final PendingBorrow pendingBorrow = pendingBorrows.poll();
            if (pendingBorrow == null) {
                // Another thread has emptied the queue since our last check, restore the count
                connection.inFlight.decrementAndGet();
            } else {
                pendingBorrowCount.decrementAndGet();
                // Ensure that the keyspace set on the connection is the one set on the pool state, in the general case it will be.
                ListenableFuture<Connection> setKeyspaceFuture = connection.setKeyspaceAsync(manager.poolsState.keyspace);
                // Slight optimization, if the keyspace was already correct the future will be complete, so simply complete it here.
                if (setKeyspaceFuture.isDone()) {
                    try {
                        if (pendingBorrow.set(Uninterruptibles.getUninterruptibly(setKeyspaceFuture))) {
                            totalInFlight.incrementAndGet();
                        } else {
                            connection.inFlight.decrementAndGet();
                        }
                    } catch (ExecutionException e) {
                        pendingBorrow.setException(e.getCause());
                        connection.inFlight.decrementAndGet();
                    }
                } else {
                    // Otherwise the keyspace did need to be set, tie the pendingBorrow future to the set keyspace completion.
                    Futures.addCallback(setKeyspaceFuture, new FutureCallback<Connection>() {

                        @Override
                        public void onSuccess(Connection c) {
                            if (pendingBorrow.set(c)) {
                                totalInFlight.incrementAndGet();
                            } else {
                                connection.inFlight.decrementAndGet();
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            pendingBorrow.setException(t);
                            connection.inFlight.decrementAndGet();
                        }
                    });
                }
            }
        }
    }

    // Trash the connection and create a new one, but we don't call trashConnection
    // directly because we want to make sure the connection is always trashed.
    private void replaceConnection(Connection connection) {
        if (!connection.state.compareAndSet(OPEN, TRASHED))
            return;
        open.decrementAndGet();
        maybeSpawnNewConnection();
        connection.maxIdleTime = Long.MIN_VALUE;
        doTrashConnection(connection);
    }

    private boolean trashConnection(Connection connection) {
        if (!connection.state.compareAndSet(OPEN, TRASHED))
            return true;

        // First, make sure we don't go below core connections
        for (; ; ) {
            int opened = open.get();
            if (opened <= options().getCoreConnectionsPerHost(hostDistance)) {
                connection.state.set(OPEN);
                return false;
            }

            if (open.compareAndSet(opened, opened - 1))
                break;
        }
        logger.trace("Trashing {}", connection);
        connection.maxIdleTime = System.currentTimeMillis() + options().getIdleTimeoutSeconds() * 1000;
        doTrashConnection(connection);
        return true;
    }

    private void doTrashConnection(Connection connection) {
        connections.remove(connection);
        trash.add(connection);
    }

    private boolean addConnectionIfUnderMaximum() {

        // First, make sure we don't cross the allowed limit of open connections
        for (; ; ) {
            int opened = open.get();
            if (opened >= options().getMaxConnectionsPerHost(hostDistance))
                return false;

            if (open.compareAndSet(opened, opened + 1))
                break;
        }

        if (phase.get() != Phase.READY) {
            open.decrementAndGet();
            return false;
        }

        // Now really open the connection
        try {
            Connection newConnection = tryResurrectFromTrash();
            if (newConnection == null) {
                if (!host.convictionPolicy.canReconnectNow()) {
                    open.decrementAndGet();
                    return false;
                }
                logger.debug("Creating new connection on busy pool to {}", host);
                newConnection = manager.connectionFactory().open(this);
                newConnection.setKeyspace(manager.poolsState.keyspace);
            }
            connections.add(newConnection);

            newConnection.state.compareAndSet(RESURRECTING, OPEN); // no-op if it was already OPEN

            // We might have raced with pool shutdown since the last check; ensure the connection gets closed in case the pool did not do it.
            if (isClosed() && !newConnection.isClosed()) {
                close(newConnection);
                open.decrementAndGet();
                return false;
            }

            dequeue(newConnection);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Skip the open but ignore otherwise
            open.decrementAndGet();
            return false;
        } catch (ConnectionException e) {
            open.decrementAndGet();
            logger.debug("Connection error to {} while creating additional connection", host);
            return false;
        } catch (AuthenticationException e) {
            // This shouldn't really happen in theory
            open.decrementAndGet();
            logger.error("Authentication error while creating additional connection (error is: {})", e.getMessage());
            return false;
        } catch (UnsupportedProtocolVersionException e) {
            // This shouldn't happen since we shouldn't have been able to connect in the first place
            open.decrementAndGet();
            logger.error("UnsupportedProtocolVersionException error while creating additional connection (error is: {})", e.getMessage());
            return false;
        } catch (ClusterNameMismatchException e) {
            open.decrementAndGet();
            logger.error("ClusterNameMismatchException error while creating additional connection (error is: {})", e.getMessage());
            return false;
        }
    }

    private Connection tryResurrectFromTrash() {
        long highestMaxIdleTime = System.currentTimeMillis();
        Connection chosen = null;

        while (true) {
            for (Connection connection : trash)
                if (connection.maxIdleTime > highestMaxIdleTime && connection.maxAvailableStreams() > minAllowedStreams) {
                    chosen = connection;
                    highestMaxIdleTime = connection.maxIdleTime;
                }

            if (chosen == null)
                return null;
            else if (chosen.state.compareAndSet(TRASHED, RESURRECTING))
                break;
        }
        logger.trace("Resurrecting {}", chosen);
        trash.remove(chosen);
        return chosen;
    }

    private void maybeSpawnNewConnection() {
        if (isClosed() || !host.convictionPolicy.canReconnectNow())
            return;

        while (true) {
            int inCreation = scheduledForCreation.get();
            if (inCreation >= MAX_SIMULTANEOUS_CREATION)
                return;
            if (scheduledForCreation.compareAndSet(inCreation, inCreation + 1))
                break;
        }

        manager.blockingExecutor().submit(newConnectionTask);
    }

    @Override
    public void onConnectionDefunct(final Connection connection) {
        if (connection.state.compareAndSet(OPEN, GONE))
            open.decrementAndGet();
        connections.remove(connection);

        // Don't try to replace the connection now. Connection.defunct already signaled the failure,
        // and either the host will be marked DOWN (which destroys all pools), or we want to prevent
        // new connections for some time
    }

    void cleanupIdleConnections(long now) {
        if (isClosed())
            return;

        shrinkIfBelowCapacity();
        cleanupTrash(now);
    }

    /**
     * If we have more active connections than needed, trash some of them
     */
    private void shrinkIfBelowCapacity() {
        int currentLoad = maxTotalInFlight.getAndSet(totalInFlight.get());

        int maxRequestsPerConnection = options().getMaxRequestsPerConnection(hostDistance);
        int needed = currentLoad / maxRequestsPerConnection + 1;
        if (currentLoad % maxRequestsPerConnection > options().getNewConnectionThreshold(hostDistance))
            needed += 1;
        needed = Math.max(needed, options().getCoreConnectionsPerHost(hostDistance));
        int actual = open.get();
        int toTrash = Math.max(0, actual - needed);

        logger.trace("Current inFlight = {}, {} connections needed, {} connections available, trashing {}",
                currentLoad, needed, actual, toTrash);

        if (toTrash <= 0)
            return;

        for (Connection connection : connections)
            if (trashConnection(connection)) {
                toTrash -= 1;
                if (toTrash == 0)
                    return;
            }
    }

    /**
     * Close connections that have been sitting in the trash for too long
     */
    private void cleanupTrash(long now) {
        for (Connection connection : trash) {
            if (connection.maxIdleTime < now && connection.state.compareAndSet(TRASHED, GONE)) {
                if (connection.inFlight.get() == 0) {
                    logger.trace("Cleaning up {}", connection);
                    trash.remove(connection);
                    close(connection);
                } else {
                    // Given that idleTimeout >> request timeout, all outstanding requests should
                    // have finished by now, so we should not get here.
                    // Restore the status so that it's retried on the next cleanup.
                    connection.state.set(TRASHED);
                }
            }
        }
    }

    private void close(final Connection connection) {
        connection.closeAsync();
    }

    final boolean isClosed() {
        return closeFuture.get() != null;
    }

    final CloseFuture closeAsync() {

        CloseFuture future = closeFuture.get();
        if (future != null)
            return future;

        phase.set(Phase.CLOSING);

        for (PendingBorrow pendingBorrow : pendingBorrows) {
            pendingBorrow.setException(new ConnectionException(host.getSocketAddress(), "Pool is shutdown"));
        }

        future = new CloseFuture.Forwarding(discardAvailableConnections());

        return closeFuture.compareAndSet(null, future)
                ? future
                : closeFuture.get(); // We raced, it's ok, return the future that was actually set
    }

    int opened() {
        return open.get();
    }

    int trashed() {
        return trash.size();
    }

    private List<CloseFuture> discardAvailableConnections() {
        // Note: if this gets called before initialization has completed, both connections and trash will be empty,
        // so this will return an empty list

        List<CloseFuture> futures = new ArrayList<CloseFuture>(connections.size() + trash.size());

        for (final Connection connection : connections) {
            CloseFuture future = connection.closeAsync();
            future.addListener(new Runnable() {
                @Override
                public void run() {
                    if (connection.state.compareAndSet(OPEN, GONE))
                        open.decrementAndGet();
                }
            }, GuavaCompatibility.INSTANCE.sameThreadExecutor());
            futures.add(future);
        }

        // Some connections in the trash might still be open if they hadn't reached their idle timeout
        for (Connection connection : trash)
            futures.add(connection.closeAsync());

        return futures;
    }

    // This creates connections if we have less than core connections (if we
    // have more than core, connection will just get trash when we can).
    void ensureCoreConnections() {
        if (isClosed())
            return;

        if (!host.convictionPolicy.canReconnectNow())
            return;

        // Note: this process is a bit racy, but it doesn't matter since we're still guaranteed to not create
        // more connection than maximum (and if we create more than core connection due to a race but this isn't
        // justified by the load, the connection in excess will be quickly trashed anyway)
        int opened = open.get();
        for (int i = opened; i < options().getCoreConnectionsPerHost(hostDistance); i++) {
            // We don't respect MAX_SIMULTANEOUS_CREATION here because it's only to
            // protect against creating connection in excess of core too quickly
            scheduledForCreation.incrementAndGet();
            manager.blockingExecutor().submit(newConnectionTask);
        }
    }

    static class PoolState {
        volatile String keyspace;

        void setKeyspace(String keyspace) {
            this.keyspace = keyspace;
        }
    }

    private class PendingBorrow {
        final SettableFuture<Connection> future;
        final Future<?> timeoutTask;

        PendingBorrow(final long timeout, final TimeUnit unit, EventExecutor timeoutsExecutor) {
            this.future = SettableFuture.create();
            this.timeoutTask = timeoutsExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    future.setException(
                            new BusyPoolException(host.getSocketAddress(), timeout, unit));
                }
            }, timeout, unit);
        }

        boolean set(Connection connection) {
            boolean succeeded = this.future.set(connection);
            this.timeoutTask.cancel(false);
            return succeeded;
        }

        void setException(Throwable exception) {
            this.future.setException(exception);
            this.timeoutTask.cancel(false);
        }
    }
}
