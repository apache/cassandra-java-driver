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


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.AuthenticationException;

class HostConnectionPool {

    private static final Logger logger = LoggerFactory.getLogger(HostConnectionPool.class);

    private static final int MAX_SIMULTANEOUS_CREATION = 1;

    // When a request timeout, we may never release its stream ID. So over time, a given connection
    // may get less an less available streams. When the number of available ones go below the
    // following threshold, we just replace the connection by a new one.
    private static final int MIN_AVAILABLE_STREAMS = 96;

    public final Host host;
    public volatile HostDistance hostDistance;
    private final SessionManager manager;

    final List<PooledConnection> connections;
    private final AtomicInteger open;
    private final Set<Connection> trash = new CopyOnWriteArraySet<Connection>();

    private volatile int waiter = 0;
    private final Lock waitLock = new ReentrantLock(true);
    private final Condition hasAvailableConnection = waitLock.newCondition();

    private final Runnable newConnectionTask;

    private final AtomicInteger scheduledForCreation = new AtomicInteger();

    private final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

    public HostConnectionPool(Host host, HostDistance hostDistance, SessionManager manager) throws ConnectionException, UnsupportedProtocolVersionException {
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

        // Create initial core connections
        List<PooledConnection> l = new ArrayList<PooledConnection>(options().getCoreConnectionsPerHost(hostDistance));
        try {
            for (int i = 0; i < options().getCoreConnectionsPerHost(hostDistance); i++)
                l.add(manager.connectionFactory().open(this));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // If asked to interrupt, we can skip opening core connections, the pool will still work.
            // But we ignore otherwise cause I'm not sure we can do much better currently.
        }
        this.connections = new CopyOnWriteArrayList<PooledConnection>(l);
        this.open = new AtomicInteger(connections.size());

        logger.trace("Created connection pool to host {}", host);
    }

    private PoolingOptions options() {
        return manager.configuration().getPoolingOptions();
    }

    public PooledConnection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        if (isClosed())
            // Note: throwing a ConnectionException is probably fine in practice as it will trigger the creation of a new host.
            // That being said, maybe having a specific exception could be cleaner.
            throw new ConnectionException(host.getSocketAddress(), "Pool is shutdown");

        if (connections.isEmpty()) {
            for (int i = 0; i < options().getCoreConnectionsPerHost(hostDistance); i++) {
                // We don't respect MAX_SIMULTANEOUS_CREATION here because it's  only to
                // protect against creating connection in excess of core too quickly
                scheduledForCreation.incrementAndGet();
                manager.blockingExecutor().submit(newConnectionTask);
            }
            PooledConnection c = waitForConnection(timeout, unit);
            c.setKeyspace(manager.poolsState.keyspace);
            return c;
        }

        int minInFlight = Integer.MAX_VALUE;
        PooledConnection leastBusy = null;
        for (PooledConnection connection : connections) {
            int inFlight = connection.inFlight.get();
            if (inFlight < minInFlight) {
                minInFlight = inFlight;
                leastBusy = connection;
            }
        }

        if (minInFlight >= options().getMaxSimultaneousRequestsPerConnectionThreshold(hostDistance) && connections.size() < options().getMaxConnectionsPerHost(hostDistance))
            maybeSpawnNewConnection();

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

    private PooledConnection waitForConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
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
            PooledConnection leastBusy = null;
            for (PooledConnection connection : connections) {
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

    public void returnConnection(PooledConnection connection) {
        if (isClosed()) {
            close(connection);
            return;
        }

        int inFlight = connection.inFlight.decrementAndGet();

        // If the connection is defunct, we have already replaced it or closed
        // the pool as part of marking it.
        if (!connection.isDefunct()) {

            if (trash.contains(connection) && inFlight == 0) {
                if (trash.remove(connection))
                    close(connection);
                return;
            }

            if (connections.size() > options().getCoreConnectionsPerHost(hostDistance) && inFlight <= options().getMinSimultaneousRequestsPerConnectionThreshold(hostDistance)) {
                trashConnection(connection);
            } else if (connection.maxAvailableStreams() < MIN_AVAILABLE_STREAMS) {
                replaceConnection(connection);
            } else {
                signalAvailableConnection();
            }
        }
    }

    // Trash the connection and create a new one, but we don't call trashConnection
    // directly because we want to make sure the connection is always trashed.
    private void replaceConnection(PooledConnection connection) {
        open.decrementAndGet();
        maybeSpawnNewConnection();
        doTrashConnection(connection);
    }

    private boolean trashConnection(PooledConnection connection) {
        // First, make sure we don't go below core connections
        for(;;) {
            int opened = open.get();
            if (opened <= options().getCoreConnectionsPerHost(hostDistance))
                return false;

            if (open.compareAndSet(opened, opened - 1))
                break;
        }

        doTrashConnection(connection);
        return true;
    }

    private void doTrashConnection(PooledConnection connection) {
        trash.add(connection);
        connections.remove(connection);

        if (connection.inFlight.get() == 0 && trash.remove(connection))
            close(connection);
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

        if (isClosed()) {
            open.decrementAndGet();
            return false;
        }

        // Now really open the connection
        try {
            connections.add(manager.connectionFactory().open(this));
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
        }
    }

    private void maybeSpawnNewConnection() {
        while (true) {
            int inCreation = scheduledForCreation.get();
            if (inCreation >= MAX_SIMULTANEOUS_CREATION)
                return;
            if (scheduledForCreation.compareAndSet(inCreation, inCreation + 1))
                break;
        }

        logger.debug("Creating new connection on busy pool to {}", host);
        manager.blockingExecutor().submit(newConnectionTask);
    }

    void replace(final Connection connection) {
        connections.remove(connection);
        connection.closeAsync();
        manager.blockingExecutor().submit(new Runnable() {
            @Override
            public void run() {
                addConnectionIfUnderMaximum();
            }
        });
    }

    private void close(final Connection connection) {
        connection.closeAsync();
    }

    public boolean isClosed() {
        return closeFuture.get() != null;
    }

    public CloseFuture closeAsync() {

        CloseFuture future = closeFuture.get();
        if (future != null)
            return future;

        // Wake up all threads that waits
        signalAllAvailableConnection();

        future = new CloseFuture.Forwarding(discardAvailableConnections());

        return closeFuture.compareAndSet(null, future)
             ? future
             : closeFuture.get(); // We raced, it's ok, return the future that was actually set
    }

    public int opened() {
        return open.get();
    }

    private List<CloseFuture> discardAvailableConnections() {

        List<CloseFuture> futures = new ArrayList<CloseFuture>(connections.size());
        for (Connection connection : connections) {
            CloseFuture future = connection.closeAsync();
            future.addListener(new Runnable() {
                public void run() {
                    open.decrementAndGet();
                }
            }, MoreExecutors.sameThreadExecutor());
            futures.add(future);
        }
        return futures;
    }

    // This creates connections if we have less than core connections (if we
    // have more than core, connection will just get trash when we can).
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

    static class PoolState {

        volatile String keyspace;

        public void setKeyspace(String keyspace) {
            this.keyspace = keyspace;
        }
    }
}
