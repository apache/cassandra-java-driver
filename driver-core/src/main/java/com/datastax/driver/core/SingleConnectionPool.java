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


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.AuthenticationException;

/**
 * A connection pool with a a single connection.
 *
 * This is used with {@link ProtocolVersion#V3} and higher.
 */
class SingleConnectionPool extends HostConnectionPool {

    private static final Logger logger = LoggerFactory.getLogger(SingleConnectionPool.class);

    // When a request timeout, we may never release its stream ID. So over time, a given connection
    // may get less an less available streams. When the number of available ones go below the
    // following threshold, we just replace the connection by a new one.
    private static final int MIN_AVAILABLE_STREAMS = 32768 * 3 / 4;

    volatile AtomicReference<PooledConnection> connectionRef = new AtomicReference<PooledConnection>();
    private final AtomicBoolean open = new AtomicBoolean();
    private final Set<Connection> trash = new CopyOnWriteArraySet<Connection>();

    private volatile int waiter = 0;
    private final Lock waitLock = new ReentrantLock(true);
    private final Condition hasAvailableConnection = waitLock.newCondition();

    private final Runnable newConnectionTask;

    private final AtomicBoolean scheduledForCreation = new AtomicBoolean();

    public SingleConnectionPool(Host host, HostDistance hostDistance, SessionManager manager) throws ConnectionException, UnsupportedProtocolVersionException, ClusterNameMismatchException {
        super(host, hostDistance, manager);

        this.newConnectionTask = new Runnable() {
            @Override
            public void run() {
                addConnectionIfNeeded();
                scheduledForCreation.set(false);
            }
        };

        // Create initial core connections
        try {
            connectionRef.set(manager.connectionFactory().open(this));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // If asked to interrupt, we can skip opening core connections, the pool will still work.
            // But we ignore otherwise cause I'm not sure we can do much better currently.
        }
        this.open.set(true);

        logger.trace("Created connection pool to host {}", host);
    }

    @Override
    public PooledConnection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        if (isClosed())
            // Note: throwing a ConnectionException is probably fine in practice as it will trigger the creation of a new host.
            // That being said, maybe having a specific exception could be cleaner.
            throw new ConnectionException(host.getSocketAddress(), "Pool is shutdown");

        PooledConnection connection = connectionRef.get();
        if (connection == null) {
            if (scheduledForCreation.compareAndSet(false, true))
                manager.blockingExecutor().submit(newConnectionTask);
            connection = waitForConnection(timeout, unit);
        } else {
            while (true) {
                int inFlight = connection.inFlight.get();

                if (inFlight >= connection.maxAvailableStreams()) {
                    connection = waitForConnection(timeout, unit);
                    break;
                }

                if (connection.inFlight.compareAndSet(inFlight, inFlight + 1))
                    break;
            }
        }
        connection.setKeyspace(manager.poolsState.keyspace);
        return connection;
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

            PooledConnection connection = connectionRef.get();
            // If we race with shutdown, connection could be null. In that case we just loop and we'll throw on the next
            // iteration anyway
            if (connection != null) {
                while (true) {
                    int inFlight = connection.inFlight.get();

                    if (inFlight >= connection.maxAvailableStreams())
                        break;

                    if (connection.inFlight.compareAndSet(inFlight, inFlight + 1))
                        return connection;
                }
            }

            remaining = timeout - Cluster.timeSince(start, unit);
        } while (remaining > 0);

        throw new TimeoutException();
    }

    @Override
    public void returnConnection(PooledConnection connection) {
        if (isClosed()) {
            close(connection);
            return;
        }

        if (connection.isDefunct()) {
            // As part of making it defunct, we have already replaced it or
            // closed the pool.
            return;
        }

        int inFlight = connection.inFlight.decrementAndGet();

        if (trash.contains(connection)) {
            if (inFlight == 0 && trash.remove(connection))
                close(connection);
        } else {
            if (connection.maxAvailableStreams() < MIN_AVAILABLE_STREAMS) {
                replaceConnection(connection);
            } else {
                signalAvailableConnection();
            }
        }
    }

    // Trash the connection and create a new one, but we don't call trashConnection
    // directly because we want to make sure the connection is always trashed.
    private void replaceConnection(PooledConnection connection) {
        if (connection.markForTrash.compareAndSet(false, true))
            open.set(false);
        maybeSpawnNewConnection();
        doTrashConnection(connection);
    }

    private void doTrashConnection(PooledConnection connection) {
        trash.add(connection);
        connectionRef.compareAndSet(connection, null);

        if (connection.inFlight.get() == 0 && trash.remove(connection))
            close(connection);
    }

    private boolean addConnectionIfNeeded() {
        if (!open.compareAndSet(false, true))
            return false;

        if (isClosed()) {
            open.set(false);
            return false;
        }

        // Now really open the connection
        try {
            connectionRef.set(manager.connectionFactory().open(this));
            signalAvailableConnection();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Skip the open but ignore otherwise
            open.set(false);
            return false;
        } catch (ConnectionException e) {
            open.set(false);
            logger.debug("Connection error to {} while creating additional connection", host);
            return false;
        } catch (AuthenticationException e) {
            // This shouldn't really happen in theory
            open.set(false);
            logger.error("Authentication error while creating additional connection (error is: {})", e.getMessage());
            return false;
        } catch (UnsupportedProtocolVersionException e) {
            // This shouldn't happen since we shouldn't have been able to connect in the first place
            open.set(false);
            logger.error("UnsupportedProtocolVersionException error while creating additional connection (error is: {})", e.getMessage());
            return false;
        } catch (ClusterNameMismatchException e) {
            open.set(false);
            logger.error("ClusterNameMismatchException error while creating additional connection (error is: {})", e.getMessage());
            return false;
        }
    }

    private void maybeSpawnNewConnection() {
        if (!scheduledForCreation.compareAndSet(false, true))
            return;

        logger.debug("Creating new connection on busy pool to {}", host);
        manager.blockingExecutor().submit(newConnectionTask);
    }

    @Override
    public void replaceDefunctConnection(final PooledConnection connection) {
        if (connection.markForTrash.compareAndSet(false, true))
            open.set(false);
        connectionRef.compareAndSet(connection, null);
        connection.closeAsync();
        manager.blockingExecutor().submit(new Runnable() {
            @Override
            public void run() {
                addConnectionIfNeeded();
            }
        });
    }

    private void close(final Connection connection) {
        connection.closeAsync();
    }

    protected CloseFuture makeCloseFuture() {
        // Wake up all threads that wait
        signalAllAvailableConnection();

        CloseFuture future = new CloseFuture.Forwarding(discardConnection());
        return future;
    }

    private List<CloseFuture> discardConnection() {

        List<CloseFuture> futures = new ArrayList<CloseFuture>();

        final PooledConnection connection = connectionRef.get();
        if (connection != null) {
            CloseFuture future = connection.closeAsync();
            future.addListener(new Runnable() {
                public void run() {
                    if (connection.markForTrash.compareAndSet(false, true))
                        open.set(false);
                }
            }, MoreExecutors.sameThreadExecutor());
            futures.add(future);
        }
        return futures;
    }

    @Override
    public void ensureCoreConnections() {
        if (isClosed())
            return;

        if (open.compareAndSet(false, true) && scheduledForCreation.compareAndSet(false, true)) {
            manager.blockingExecutor().submit(newConnectionTask);
        }
    }

    @Override
    public int opened() {
        return open.get() ? 1 : 0;
    }

    @Override
    public int inFlightQueriesCount() {
        PooledConnection connection = connectionRef.get();
        return connection == null ? 0 : connection.inFlight.get();
    }
}
