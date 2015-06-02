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


import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.utils.MoreFutures;

import static com.datastax.driver.core.Connection.State.*;

/**
 * A connection pool with a varying number of connections.
 *
 * This is used with {@link ProtocolVersion#V2} and lower.
 */
class DynamicConnectionPool extends HostConnectionPool {

    private static final Logger logger = LoggerFactory.getLogger(DynamicConnectionPool.class);

    private static final int MAX_SIMULTANEOUS_CREATION = 1;

    // When a request timeout, we may never release its stream ID. So over time, a given connection
    // may get less an less available streams. When the number of available ones go below the
    // following threshold, we just replace the connection by a new one.
    private static final int MIN_AVAILABLE_STREAMS = 96;

    final List<Connection> connections;
    private final AtomicInteger open;
    /** The total number of in-flight requests on all connections of this pool. */
    final AtomicInteger totalInFlight = new AtomicInteger();
    /** The maximum value of {@link #totalInFlight} since the last call to {@link #cleanupIdleConnections(long)}*/
    private final AtomicInteger maxTotalInFlight = new AtomicInteger();

    @VisibleForTesting
    final Set<Connection> trash = new CopyOnWriteArraySet<Connection>();

    private volatile int waiter = 0;
    private final Lock waitLock = new ReentrantLock(true);
    private final Condition hasAvailableConnection = waitLock.newCondition();

    private final Runnable newConnectionTask;

    private final AtomicInteger scheduledForCreation = new AtomicInteger();

    public DynamicConnectionPool(Host host, HostDistance hostDistance, SessionManager manager) {
        super(host, hostDistance, manager);

        this.newConnectionTask = new Runnable() {
            @Override
            public void run() {
                addConnectionIfUnderMaximum();
                scheduledForCreation.decrementAndGet();
            }
        };

        this.connections = new CopyOnWriteArrayList<Connection>();
        this.open = new AtomicInteger();
    }

    @Override
    ListenableFuture<Void> initAsync(Connection reusedConnection) {
        // Create initial core connections
        int capacity = options().getCoreConnectionsPerHost(hostDistance);
        final List<Connection> connections = Lists.newArrayListWithCapacity(capacity);
        final List<ListenableFuture<Void>> connectionFutures = Lists.newArrayListWithCapacity(capacity);
        for (int i = 0; i < capacity; i++) {
            Connection connection;
            ListenableFuture<Void> connectionFuture;
            // reuse the existing connection only once
            if (reusedConnection != null && reusedConnection.setPool(this)) {
                connection = reusedConnection;
                connectionFuture = MoreFutures.VOID_SUCCESS;
            } else {
                connection = manager.connectionFactory().newConnection(this);
                connectionFuture = connection.initAsync();
            }
            reusedConnection = null;
            connections.add(connection);
            connectionFutures.add(connectionFuture);
        }

        Executor initExecutor = manager.cluster.manager.configuration.getPoolingOptions().getInitializationExecutor();

        ListenableFuture<List<Void>> allConnectionsFuture = Futures.allAsList(connectionFutures);

        final SettableFuture<Void> initFuture = SettableFuture.create();
        Futures.addCallback(allConnectionsFuture, new FutureCallback<List<Void>>() {
            @Override
            public void onSuccess(List<Void> l) {
                DynamicConnectionPool.this.connections.addAll(connections);
                open.set(l.size());
                if (isClosed()) {
                    initFuture.setException(new ConnectionException(host.getSocketAddress(), "Pool was closed during initialization"));
                    // we're not sure if closeAsync() saw the connections, so ensure they get closed
                    forceClose(connections);
                } else {
                    logger.trace("Created connection pool to host {}", host);
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

    // Clean up if we got an error at construction time but still created part of the core connections
    private void forceClose(List<Connection> connections) {
        for (Connection connection : connections) {
            connection.closeAsync().force();
        }
    }

    private PoolingOptions options() {
        return manager.configuration().getPoolingOptions();
    }

    @Override
    public Connection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        Phase phase = this.phase.get();
        if (phase != Phase.READY)
            // Note: throwing a ConnectionException is probably fine in practice as it will trigger the creation of a new host.
            // That being said, maybe having a specific exception could be cleaner.
            throw new ConnectionException(host.getSocketAddress(), "Pool is " + phase);

        if (connections.isEmpty()) {
            for (int i = 0; i < options().getCoreConnectionsPerHost(hostDistance); i++) {
                // We don't respect MAX_SIMULTANEOUS_CREATION here because it's  only to
                // protect against creating connection in excess of core too quickly
                scheduledForCreation.incrementAndGet();
                manager.blockingExecutor().submit(newConnectionTask);
            }
            Connection c = waitForConnection(timeout, unit);
            totalInFlight.incrementAndGet();
            c.setKeyspace(manager.poolsState.keyspace);
            return c;
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
                throw new ConnectionException(host.getSocketAddress(), "Pool is shutdown");
            // This might maybe happen if the number of core connections per host is 0 and a connection was trashed between
            // the previous check to connections and now. But in that case, the line above will have trigger the creation of
            // a new connection, so just wait that connection and move on
            leastBusy = waitForConnection(timeout, unit);
        } else {
            while (true) {
                int inFlight = leastBusy.inFlight.get();

                if (inFlight >= leastBusy.maxAvailableStreams()) {
                    leastBusy = waitForConnection(timeout, unit);
                    break;
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
        if (connectionCount < options().getMaxConnectionsPerHost(hostDistance)) {
            // Add a connection if we fill the first n-1 connections and almost fill the last one
            int currentCapacity = (connectionCount - 1) * StreamIdGenerator.MAX_STREAM_PER_CONNECTION_V2
                + options().getMaxSimultaneousRequestsPerConnectionThreshold(hostDistance);
            if (totalInFlightCount > currentCapacity)
                maybeSpawnNewConnection();
        }

        leastBusy.setKeyspace(manager.poolsState.keyspace);
        return leastBusy;
    }

    private void awaitAvailableConnection(long timeout, TimeUnit unit) throws InterruptedException {
        waitLock.lock();
        waiter++;
        try {
            hasAvailableConnection.await(timeout, unit);
        } finally {
            waiter--;
            waitLock.unlock();
        }
    }

    private void signalAvailableConnection() {
        // Quick check if it's worth signaling to avoid locking
        if (waiter == 0)
            return;

        waitLock.lock();
        try {
            hasAvailableConnection.signal();
        } finally {
            waitLock.unlock();
        }
    }

    private void signalAllAvailableConnection() {
        // Quick check if it's worth signaling to avoid locking
        if (waiter == 0)
            return;

        waitLock.lock();
        try {
            hasAvailableConnection.signalAll();
        } finally {
            waitLock.unlock();
        }
    }

    private Connection waitForConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        if (timeout == 0)
            throw new TimeoutException();

        long start = System.nanoTime();
        long remaining = timeout;
        do {
            try {
                awaitAvailableConnection(remaining, unit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // If we're interrupted fine, check if there is a connection available but stop waiting otherwise
                timeout = 0; // this will make us stop the loop if we don't get a connection right away
            }

            if (isClosed())
                throw new ConnectionException(host.getSocketAddress(), "Pool is shutdown");

            int minInFlight = Integer.MAX_VALUE;
            Connection leastBusy = null;
            for (Connection connection : connections) {
                int inFlight = connection.inFlight.get();
                if (inFlight < minInFlight) {
                    minInFlight = inFlight;
                    leastBusy = connection;
                }
            }

            // If we race with shutdown, leastBusy could be null. In that case we just loop and we'll throw on the next
            // iteration anyway
            if (leastBusy != null) {
                while (true) {
                    int inFlight = leastBusy.inFlight.get();

                    if (inFlight >= leastBusy.maxAvailableStreams())
                        break;

                    if (leastBusy.inFlight.compareAndSet(inFlight, inFlight + 1))
                        return leastBusy;
                }
            }

            remaining = timeout - Cluster.timeSince(start, unit);
        } while (remaining > 0);

        throw new TimeoutException();
    }

    @Override
    public void returnConnection(Connection connection) {
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
            if (connection.maxAvailableStreams() < MIN_AVAILABLE_STREAMS) {
                replaceConnection(connection);
            } else {
                signalAvailableConnection();
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
        for (;;) {
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
        for(;;) {
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
                logger.debug("Creating new connection on busy pool to {}", host);
                newConnection = manager.connectionFactory().open(this);
            }
            connections.add(newConnection);

            newConnection.state.compareAndSet(RESURRECTING, OPEN); // no-op if it was already OPEN

            // We might have raced with pool shutdown since the last check; ensure the connection gets closed in case the pool did not do it.
            if (isClosed() && !newConnection.isClosed()) {
                close(newConnection);
                open.decrementAndGet();
                return false;
            }

            signalAvailableConnection();
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
                if (connection.maxIdleTime > highestMaxIdleTime && connection.maxAvailableStreams() > MIN_AVAILABLE_STREAMS) {
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
    void replaceDefunctConnection(final Connection connection) {
        if (connection.state.compareAndSet(OPEN, GONE))
            open.decrementAndGet();
        if (connections.remove(connection))
            manager.blockingExecutor().submit(new Runnable() {
                @Override
                public void run() {
                    addConnectionIfUnderMaximum();
                }
            });
    }

    @Override
    void cleanupIdleConnections(long now) {
        if (isClosed())
            return;

        shrinkIfBelowCapacity();
        cleanupTrash(now);
    }

    /** If we have more active connections than needed, trash some of them */
    private void shrinkIfBelowCapacity() {
        int currentLoad = maxTotalInFlight.getAndSet(totalInFlight.get());

        int needed = currentLoad / StreamIdGenerator.MAX_STREAM_PER_CONNECTION_V2 + 1;
        if (currentLoad % StreamIdGenerator.MAX_STREAM_PER_CONNECTION_V2 > options().getMaxSimultaneousRequestsPerConnectionThreshold(hostDistance))
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

    /** Close connections that have been sitting in the trash for too long */
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

    protected CloseFuture makeCloseFuture() {
        // Wake up all threads that wait
        signalAllAvailableConnection();

        return new CloseFuture.Forwarding(discardAvailableConnections());
    }

    private List<CloseFuture> discardAvailableConnections() {
        // Note: if this gets called before initialization has completed, both connections and trash will be empty,
        // so this will return an empty list

        List<CloseFuture> futures = new ArrayList<CloseFuture>(connections.size() + trash.size());

        for (final Connection connection : connections) {
            CloseFuture future = connection.closeAsync();
            future.addListener(new Runnable() {
                public void run() {
                    if (connection.state.compareAndSet(OPEN, GONE))
                        open.decrementAndGet();
                }
            }, MoreExecutors.sameThreadExecutor());
            futures.add(future);
        }

        // Some connections in the trash might still be open if they hadn't reached their idle timeout
        for (Connection connection : trash)
            futures.add(connection.closeAsync());

        return futures;
    }

    // This creates connections if we have less than core connections (if we
    // have more than core, connection will just get trash when we can).
    @Override
    public void ensureCoreConnections() {
        if (isClosed())
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

    @Override
    public int opened() {
        return open.get();
    }

    @Override
    int trashed() {
        return trash.size();
    }

    @Override
    public int inFlightQueriesCount() {
        int count = 0;
        for (Connection connection : connections) {
            count += connection.inFlight.get();
        }
        return count;
    }
}
