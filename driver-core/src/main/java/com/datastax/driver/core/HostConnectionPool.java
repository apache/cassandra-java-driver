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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.AuthenticationException;

class HostConnectionPool {

    private static final Logger logger = LoggerFactory.getLogger(HostConnectionPool.class);

    private static final int MAX_SIMULTANEOUS_CREATION = 1;

    public final Host host;
    public volatile HostDistance hostDistance;
    private final Session.Manager manager;

    private final List<Connection> connections;
    private final AtomicInteger open;
    private final AtomicBoolean isShutdown = new AtomicBoolean();
    private final Set<Connection> trash = new CopyOnWriteArraySet<Connection>();

    private final Lock waitLock = new ReentrantLock(true);
    private final Condition hasAvailableConnection = waitLock.newCondition();

    private final Runnable newConnectionTask;

    private final AtomicInteger scheduledForCreation = new AtomicInteger();

    public HostConnectionPool(Host host, HostDistance hostDistance, Session.Manager manager) throws ConnectionException {
        this.host = host;
        this.hostDistance = hostDistance;
        this.manager = manager;

        this.newConnectionTask = new Runnable() {
            public void run() {
                addConnectionIfUnderMaximum();
                scheduledForCreation.decrementAndGet();
            }
        };

        // Create initial core connections
        List<Connection> l = new ArrayList<Connection>(options().getCoreConnectionsPerHost(hostDistance));
        try {
            for (int i = 0; i < options().getCoreConnectionsPerHost(hostDistance); i++)
                l.add(manager.connectionFactory().open(host));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // If asked to interrupt, we can skip opening core connections, the pool will still work.
            // But we ignore otherwise cause I'm not sure we can do much better currently.
        }
        this.connections = new CopyOnWriteArrayList<Connection>(l);
        this.open = new AtomicInteger(connections.size());

        logger.trace("Created connection pool to host {}", host);
    }

    private PoolingOptions options() {
        return manager.configuration().getPoolingOptions();
    }

    public Connection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        if (isShutdown.get())
            // Note: throwing a ConnectionException is probably fine in practice as it will trigger the creation of a new host.
            // That being said, maybe having a specific exception could be cleaner.
            throw new ConnectionException(host.getAddress(), "Pool is shutdown");

        if (connections.isEmpty()) {
            for (int i = 0; i < options().getCoreConnectionsPerHost(hostDistance); i++) {
                // We don't respect MAX_SIMULTANEOUS_CREATION here because it's  only to
                // protect against creating connection in excess of core too quickly
                scheduledForCreation.incrementAndGet();
                manager.executor().submit(newConnectionTask);
            }
            Connection c = waitForConnection(timeout, unit);
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

        if (minInFlight >= options().getMaxSimultaneousRequestsPerConnectionTreshold(hostDistance) && connections.size() < options().getMaxConnectionPerHost(hostDistance))
            maybeSpawnNewConnection();

        while (true) {
            int inFlight = leastBusy.inFlight.get();

            if (inFlight >= Connection.MAX_STREAM_PER_CONNECTION) {
                leastBusy = waitForConnection(timeout, unit);
                break;
            }

            if (leastBusy.inFlight.compareAndSet(inFlight, inFlight + 1))
                break;
        }
        leastBusy.setKeyspace(manager.poolsState.keyspace);
        return leastBusy;
    }

    private static long elapsed(long start, TimeUnit unit) {
        return unit.convert(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    }

    private void awaitAvailableConnection(long timeout, TimeUnit unit) throws InterruptedException {
        waitLock.lock();
        try {
            hasAvailableConnection.await(timeout, unit);
        } finally {
            waitLock.unlock();
        }
    }

    private void signalAvailableConnection() {
        waitLock.lock();
        try {
            hasAvailableConnection.signal();
        } finally {
            waitLock.unlock();
        }
    }

    private void signalAllAvailableConnection() {
        waitLock.lock();
        try {
            hasAvailableConnection.signal();
        } finally {
            waitLock.unlock();
        }
    }

    private Connection waitForConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException {
        long start = System.currentTimeMillis();
        long remaining = timeout;
        do {
            try {
                awaitAvailableConnection(remaining, unit);
            } catch (InterruptedException e) {
                Thread.interrupted();
                // If we're interrupted fine, check if there is a connection available but stop waiting otherwise
                timeout = 0; // this will make us stop the loop if we don't get a connection right away
            }

            if (isShutdown())
                throw new ConnectionException(host.getAddress(), "Pool is shutdown");

            int minInFlight = Integer.MAX_VALUE;
            Connection leastBusy = null;
            for (Connection connection : connections) {
                int inFlight = connection.inFlight.get();
                if (inFlight < minInFlight) {
                    minInFlight = inFlight;
                    leastBusy = connection;
                }
            }

            while (true) {
                int inFlight = leastBusy.inFlight.get();

                if (inFlight >= Connection.MAX_STREAM_PER_CONNECTION)
                    break;

                if (leastBusy.inFlight.compareAndSet(inFlight, inFlight + 1))
                    return leastBusy;
            }

            remaining = timeout - elapsed(start, unit);
        } while (remaining > 0);

        throw new TimeoutException();
    }

    public void returnConnection(Connection connection) {
        int inFlight = connection.inFlight.decrementAndGet();

        if (connection.isDefunct()) {
            if (host.getMonitor().signalConnectionFailure(connection.lastException()))
                shutdown();
            else
                replace(connection);
        } else {

            if (trash.contains(connection) && inFlight == 0) {
                if (trash.remove(connection))
                    close(connection);
                return;
            }

            if (connections.size() > options().getCoreConnectionsPerHost(hostDistance) && inFlight <= options().getMinSimultaneousRequestsPerConnectionTreshold(hostDistance)) {
                trashConnection(connection);
            } else {
                signalAvailableConnection();
            }
        }
    }

    private boolean trashConnection(Connection connection) {
        // First, make sure we don't go below core connections
        for(;;) {
            int opened = open.get();
            if (opened <= options().getCoreConnectionsPerHost(hostDistance))
                return false;

            if (open.compareAndSet(opened, opened - 1))
                break;
        }
        trash.add(connection);
        connections.remove(connection);

        if (connection.inFlight.get() == 0 && trash.remove(connection))
            close(connection);
        return true;
    }

    private boolean addConnectionIfUnderMaximum() {

        // First, make sure we don't cross the allowed limit of open connections
        for(;;) {
            int opened = open.get();
            if (opened >= options().getMaxConnectionPerHost(hostDistance))
                return false;

            if (open.compareAndSet(opened, opened + 1))
                break;
        }

        if (isShutdown()) {
            open.decrementAndGet();
            return false;
        }

        // Now really open the connection
        try {
            connections.add(manager.connectionFactory().open(host));
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
            if (host.getMonitor().signalConnectionFailure(e))
                shutdown();
            return false;
        } catch (AuthenticationException e) {
            // This shouldn't really happen in theory
            open.decrementAndGet();
            logger.error("Authentication error while creating additional connection (error is: {})", e.getMessage());
            shutdown();
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
        manager.executor().submit(newConnectionTask);
    }

    private void replace(final Connection connection) {
        connections.remove(connection);

        manager.executor().submit(new Runnable() {
            public void run() {
                connection.close();
                addConnectionIfUnderMaximum();
            }
        });
    }

    private void close(final Connection connection) {
        manager.executor().submit(new Runnable() {
            public void run() {
                connection.close();
            }
        });
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }

    public void shutdown() {
        if (!isShutdown.compareAndSet(false, true))
            return;

        logger.debug("Shutting down pool");

        // Wake up all threads that waits
        signalAllAvailableConnection();
        discardAvailableConnections();
    }

    public int opened() {
        return open.get();
    }

    private void discardAvailableConnections() {
        for (Connection connection : connections) {
            connection.close();
            open.decrementAndGet();
        }
    }

    // This creates connections if we have less than core connections (if we
    // have more than core, connection will just get trash when we can).
    public void ensureCoreConnections() {
        if (isShutdown())
            return;

        // Note: this process is a bit racy, but it doesn't matter since we're still guaranteed to not create
        // more connection than maximum (and if we create more than core connection due to a race but this isn't
        // justified by the load, the connection in excess will be quickly trashed anyway)
        int opened = open.get();
        for (int i = opened; i < options().getCoreConnectionsPerHost(hostDistance); i++) {
            // We don't respect MAX_SIMULTANEOUS_CREATION here because it's only to
            // protect against creating connection in excess of core too quickly
            scheduledForCreation.incrementAndGet();
            manager.executor().submit(newConnectionTask);
        }
    }

    static class PoolState {

        volatile String keyspace;

        public void setKeyspace(String keyspace) {
            this.keyspace = keyspace;
        }
    }
}
